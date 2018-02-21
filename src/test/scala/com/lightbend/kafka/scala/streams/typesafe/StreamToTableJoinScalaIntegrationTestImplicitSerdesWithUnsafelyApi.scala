/**
  * Copyright (C) 2018 OpenShine SL <https://www.openshine.com>
  * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
  * Adapted from Confluent Inc. whose copyright is reproduced below.
  */

/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lightbend.kafka.scala.streams.typesafe

import java.util.Properties

import com.lightbend.kafka.scala.server.{KafkaLocalServer, MessageListener, MessageSender, RecordProcessorTrait}
import com.lightbend.kafka.scala.streams.StreamToTableJoinTestData
import minitest.TestSuite
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{Joined, KStream}

/**
  * @author Santiago Saavedra (ssaavedra@openshine.com)
  */
object StreamToTableJoinScalaIntegrationTestImplicitSerdesWithUnsafelyApi
  extends TestSuite[KafkaLocalServer] with StreamToTableJoinTestData {

  /**
    * End-to-end integration test that demonstrates how to perform a join
    * between a KStream and a
    * KTable (think: KStream.leftJoin(KTable)), i.e. an example of a stateful
    * computation.
    *
    * See StreamToTableJoinIntegrationTest for the equivalent Java example.
    *
    * Note: We intentionally use JUnit4 (wrapped by ScalaTest) for
    * implementing this Scala integration
    * test so it is easier to compare this Scala code with the equivalent
    * Java code at
    * StreamToTableJoinIntegrationTest.  One difference is that, to simplify
    * the Scala/Junit integration, we
    * switched from BeforeClass (which must be `static`) to Before as well as
    * from @ClassRule (which
    * must be `static` and `public`) to a workaround combination of `@Rule
    * def` and a `private val`.
    */


  override def setup(): KafkaLocalServer = {
    val s = KafkaLocalServer(true, Some(localStateDir))
    s.start()
    s
  }

  override def tearDown(server: KafkaLocalServer): Unit = {
    server.stop()
  }

  test("should count clicks per region") { server =>

    server.createTopic(userClicksTopic)
    server.createTopic(userRegionsTopic)
    server.createTopic(outputTopic)

    //
    // Step 1: Configure and start the processor topology.
    //
    val streamsConfiguration: Properties = {
      val p = new Properties()
      p
        .put(StreamsConfig.APPLICATION_ID_CONFIG,
          s"stream-table-join-scala-integration-test-implicit-serdes-${
            scala
              .util.Random.nextInt(100)
          }")
      p
        .put(StreamsConfig.CLIENT_ID_CONFIG,
          "join-scala-integration-test-implicit-serdes-standard-consumer")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100")
      p.put(StreamsConfig.STATE_DIR_CONFIG, localStateDir)
      p
    }

    import ImplicitConverters._
    import SerdeDerivations._
    import com.lightbend.kafka.scala.streams.FunctionConversions._
    import unsafe._
    implicit val builder = new StreamsBuilder()

    val userClicksStream: TSKStream[String, Long] = TSKStream(userClicksTopic)

    val userRegionsTable: TSKTable[String, String] = TSKTable(userRegionsTopic)


    // Compute the total per region by summing the individual click counts
    // per region.
    val clicksPerRegion: TSKTable[String, Long] =
    userClicksStream

      // Join the stream against the table.
      // Here, we use the `unsafely` API to work on the underlying object
      // while still returning a typesafe result. We could remove the type
      // annotation in the unsafely call, but it helps IDEs to find the
      // appropriate return value when chaining calls afterwards.
      .unsafely[String, (String, Long), KStream, TSKStream] {
      _.leftJoin[String, (String, Long)](
        userRegionsTable.unsafe, {
          (clicks: Long, region: String) =>
            (if (region == null) "UNKNOWN" else region, clicks)
        }.asValueJoiner,
        Joined
          .keySerde[String, Long, String](stringSerde)
          .withValueSerde(scalaLongSerde))
    }
      // Change the stream from <user> -> <region, clicks> to <region> ->
      // <clicks>
      .map((_, regionWithClicks) => regionWithClicks)

      // Compute the total per region by summing the individual click
      // counts per region.
      .groupByKey
      .reduce(_ + _)

    // Write the (continuously updating) results to the output topic.
    clicksPerRegion.toStream.to(outputTopic)

    val streams: KafkaStreams = new KafkaStreams(builder.build,
      streamsConfiguration)

    streams
      .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
        override def uncaughtException(t: Thread, e: Throwable): Unit = try {
          println(
            s"Stream terminated because of uncaught exception .. Shutting " +
              s"down app",

            e)
          e.printStackTrace
          val closed = streams.close()
          println(s"Exiting application after streams close ($closed)")
        } catch {
          case x: Exception => x.printStackTrace
        } finally {
          println("Exiting application ..")
          System.exit(-1)
        }
      })

    streams.start()

    //
    // Step 2: Publish user-region information.
    //
    // To keep this code example simple and easier to understand/reason
    // about, we publish all
    // user-region records before any user-click records (cf. step 3).  In
    // practice though,
    // data records would typically be arriving concurrently in both input
    // streams/topics.
    val sender1 = MessageSender[String, String](brokers,
      classOf[StringSerializer].getName, classOf[StringSerializer].getName)
    userRegions
      .foreach(r => sender1.writeKeyValue(userRegionsTopic, r.key, r.value))

    //
    // Step 3: Publish some user click events.
    //
    val sender2 = MessageSender[String, Long](brokers,
      classOf[StringSerializer].getName, classOf[LongSerializer].getName)
    userClicks
      .foreach(r => sender2.writeKeyValue(userClicksTopic, r.key, r.value))

    //
    // Step 4: Verify the application's output data.
    //
    val listener = MessageListener(brokers, outputTopic,
      "join-scala-integration-test-standard-consumer",
      classOf[StringDeserializer].getName,
      classOf[LongDeserializer].getName,
      new RecordProcessor
    )

    val l = listener
      .waitUntilMinKeyValueRecordsReceived(expectedClicksPerRegion.size,
        30000)
    streams.close()
    assertEquals(l.sortBy(_.key), expectedClicksPerRegion.sortBy(_.key))
  }

  class RecordProcessor extends RecordProcessorTrait[String, Long] {
    override def processRecord(record: ConsumerRecord[String, Long]): Unit = {
      // println(s"Get Message $record")
    }
  }

}
