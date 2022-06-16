package sample.sharding.kafka

import akka.Done
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, Terminated}
import akka.cluster.typed.{Cluster, SelfUp, Subscribe}
import akka.http.scaladsl._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.kafka.scaladsl.Producer
import akka.management.scaladsl.AkkaManagement
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.utils.Utils
import sample.sharding.kafka.serialization.user_events.UserPurchaseProto
import sharding.kafka.producer.EventsProducer
import sharding.kafka.producer.UserEventProducer.{log, maxPrice, maxQuantity, nrUsers, producerConfig, producerSettings, products}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Random, Success}
sealed trait Command
case object NodeMemberUp extends Command
final case class ShardingStarted(region: ActorRef[UserEvents.Command]) extends Command
final case class BindingFailed(reason: Throwable) extends Command
class ProcessRunner{
  def init(remotingPort: Int, akkaManagementPort: Int, frontEndPort: Int): Unit = {
    ActorSystem(Behaviors.setup[Command] {
      ctx =>
        AkkaManagement(ctx.system.toClassic).start()
        val cluster = Cluster(ctx.system)
        val upAdapter = ctx.messageAdapter[SelfUp](_ => NodeMemberUp)
        cluster.subscriptions ! Subscribe(upAdapter, classOf[SelfUp])
        val settings = ProcessorSettings("kafka-to-sharding-processor", ctx.system.toClassic)
        ctx.pipeToSelf(UserEvents.init(ctx.system, settings)) {
          case Success(extractor) => ShardingStarted(extractor)
          case Failure(ex) => throw ex
        }
        starting(ctx, None, joinedCluster = false, settings)
    }, "KafkaToSharding", config(remotingPort, akkaManagementPort))

    def start(ctx: ActorContext[Command], region: ActorRef[UserEvents.Command], settings: ProcessorSettings): Behavior[Command] = {
      import ctx.executionContext
      ctx.log.info("Sharding started and joined cluster. Starting event processor")
      val eventProcessor = ctx.spawn[Nothing](UserEventsKafkaProcessor(region, settings), "kafka-event-processor")
      val binding: Future[Http.ServerBinding] = startGrpc(ctx.system, frontEndPort, region)
      binding.onComplete {
        case Failure(t) =>
          ctx.self ! BindingFailed(t)
        case _ =>
      }
      running(ctx, binding, eventProcessor)
    }

    def starting(ctx: ActorContext[Command], sharding: Option[ActorRef[UserEvents.Command]], joinedCluster: Boolean, settings: ProcessorSettings): Behavior[Command] = Behaviors
      .receive[Command] {
        case (ctx, ShardingStarted(region)) if joinedCluster =>
          ctx.log.info("Sharding has started")
          start(ctx, region, settings)
        case (_, ShardingStarted(region)) =>
          ctx.log.info("Sharding has started")
          starting(ctx, Some(region), joinedCluster, settings)
        case (ctx, NodeMemberUp) if sharding.isDefined =>
          ctx.log.info("Member has joined the cluster")
          start(ctx, sharding.get, settings)
        case (_, NodeMemberUp)  =>
          ctx.log.info("Member has joined the cluster")
          starting(ctx, sharding, joinedCluster = true, settings)
      }

    def running(ctx: ActorContext[Command], binding: Future[Http.ServerBinding], processor: ActorRef[Nothing]): Behavior[Command] =
      Behaviors.receiveMessagePartial[Command] {
        case BindingFailed(t) =>
          ctx.log.error("Failed to bind front end", t)
          Behaviors.stopped
      }.receiveSignal {
        case (ctx, Terminated(`processor`)) =>
          ctx.log.warn("Kafka event processor stopped. Shutting down")
          binding.map(_.unbind())(ctx.executionContext)
          Behaviors.stopped
      }


    def startGrpc(system: ActorSystem[_], frontEndPort: Int, region: ActorRef[UserEvents.Command]): Future[Http.ServerBinding] = {
      val mat = Materializer.createMaterializer(system.toClassic)
      val service: HttpRequest => Future[HttpResponse] =
        UserServiceHandler(new UserGrpcService(system, region))(mat, system.toClassic)
      Http()(system.toClassic).bindAndHandleAsync(
        service,
        interface = "127.0.0.1",
        port = frontEndPort,
        connectionContext = HttpConnectionContext())(mat)

    }

    def config(port: Int, managementPort: Int): Config =
      ConfigFactory.parseString(s"""
      akka.remote.artery.canonical.port = $port
      akka.management.http.port = $managementPort
       """).withFallback(ConfigFactory.load())

  }
  def isInt(s: String): Boolean = s.matches("""\d+""")

}

object ProcessorActor extends ProcessRunner {
  final case class Start(args:Array[String])
  def apply(): Behavior[Start] =
    Behaviors.receiveMessage{message =>
      println(s"Starting runner")
      startRunner(message.args)
      Behaviors.same
    }

  def startRunner(args: Array[String]) = {
    args.toList match {
      case single :: Nil if isInt(single) =>
        val nr = single.toInt
        init(2550 + nr, 8550 + nr, 8080 + nr)
      case portString :: managementPort :: frontEndPort :: Nil
        if isInt(portString) && isInt(managementPort) && isInt(frontEndPort) =>
        init(portString.toInt, managementPort.toInt, frontEndPort.toInt)
      case _ =>
        throw new IllegalArgumentException("usage: <remotingPort> <managementPort> <frontEndPort>")
    }
  }
}
trait cacheForTests {
  def addToCache(partition:Int, message:UserPurchaseProto, map:mutable.HashMap[Int, mutable.Set[UserPurchaseProto]]) = {
    val ex = map.getOrElse(partition, mutable.Set[UserPurchaseProto]())
    ex.+=(message)
    map.put(partition, ex)
  }
}
object ProducerActor extends EventsProducer {
  final case class Start(args:Array[String])
  def apply(): Behavior[Start] =
    Behaviors.receiveMessage{message =>
      println(s"Starting runner")
      val done: Future[Done] =
        Source//(1 to 10000)
          .tick(1.second, 5.millis, "tick")
          .map(_ => {
            val randomEntityId = Random.nextInt(nrUsers).toString
            val price = Random.nextInt(maxPrice)
            val quantity = Random.nextInt(maxQuantity)
            val product = products(Random.nextInt(products.size))
            val messageObj = UserPurchaseProto(randomEntityId, product, quantity, price)
            val message = messageObj.toByteArray
            log.info("Sending message to user {}", randomEntityId)
            // rely on the default kafka partitioner to hash the key and distribute among shards
            // the logic of the default partitioner must be replicated in MessageExtractor entityId -> shardId function
            new ProducerRecord[String, Array[Byte]](producerConfig.topic, randomEntityId, message)
          })
          .runWith(Producer.plainSink(producerSettings))

      Behaviors.same
    }
}

/*object Main extends ProcessRunner {
  def main(args: Array[String]): Unit = {
    val testkit = ActorTestKit()
    val system: ActorSystem[ProcessorActor.Start] = ActorSystem(ProcessorActor(), "processor")
    system ! ProcessorActor.Start(Array())
    Thread.sleep(5000)
    system ! ProcessorActor.Start(Array())
    Thread.sleep(5000)

    /*args.toList match {
      case single :: Nil if isInt(single) =>
        val nr = single.toInt
        init(2550 + nr, 8550 + nr, 8080 + nr)
      case portString :: managementPort :: frontEndPort :: Nil
          if isInt(portString) && isInt(managementPort) && isInt(frontEndPort) =>
        init(portString.toInt, managementPort.toInt, frontEndPort.toInt)
      case _ =>
        throw new IllegalArgumentException("usage: <remotingPort> <managementPort> <frontEndPort>")
    }*/
  }
}*/
