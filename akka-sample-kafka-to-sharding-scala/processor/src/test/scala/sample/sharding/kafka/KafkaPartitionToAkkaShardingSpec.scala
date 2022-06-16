package sample.sharding.kafka

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorSystem
import org.scalatest.{Args, AsyncWordSpec, BeforeAndAfterAll, ConfigMap, Filter, Matchers, Status, Suite, TestData}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

// MUST CREATE KAFKA TOPIC MANUALLY WITH 10 PARTITIONS
class KafkaPartitionToAkkaShardingSpec extends AsyncWordSpec with  ScalaFutures with Eventually with Matchers with BeforeAndAfterAll{
  private def waitBeforeValidation(duration: FiniteDuration = 10.seconds): Unit = Thread.sleep(duration.toMillis)
  val system: ActorSystem[ProcessorActor.Start] = ActorSystem(ProcessorActor(), "processor")

  val testKit = ActorTestKit()

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
  }
  "The Kafka-partition-to-akka-shared spec " should {
    "demo rebalancing of akka cluster and kafka-partition-to-akka-shard stickness" in {
      /* start processor 1*/
      val p1 = testKit.spawn(ProcessorActor(), "processor1")
      p1 ! ProcessorActor.Start(Array("2551", "8551", "8081"))
      waitBeforeValidation(30.seconds)
      /*start producer for kafka topic*/
      val producer = testKit.spawn(ProducerActor(), "producer")
      producer ! ProducerActor.Start(Array())
      waitBeforeValidation(30.seconds)
      /* start processor 2 - to see rebalancing*/
      val p2 = testKit.spawn(ProcessorActor(), "processor2")
      p2 ! ProcessorActor.Start(Array("2552", "8552", "8082"))
      waitBeforeValidation(60.seconds)
      /* stop processor 2 to again see rebalancing*/
      testKit.stop(p2)
      waitBeforeValidation(60.seconds)
      /* stop producer */
      testKit.stop(producer)
      waitBeforeValidation(60.seconds)
      /* stop processor 1 - finishing*/
      testKit.stop(p1)
      val producerRecs = UserEventsKafkaProcessor.producerEventsByPartition
      val processorRecs = UserEvents.producerEventsByPartition

      /* Rebalancing can only be seen in the logs */

      // verify what produced in kafka is in sync with what processed by akka-shards. each kafka-partition goes to akka-shard.
      producerRecs.keySet should be(processorRecs.keySet)
      producerRecs.keySet.foreach(partition =>{
        val diff = processorRecs(partition).diff(producerRecs(partition))
        assert(diff.isEmpty)
        println(s"partition/shard: $partition, diff: $diff")
      })
      producerRecs.toList should contain allElementsOf  processorRecs.toList
    }
  }
}
