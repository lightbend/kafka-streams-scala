/**
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
package com.lightbend.kafka.scala.streams

import java.util.Properties
import minitest.TestSuite
import com.lightbend.kafka.scala.server.{ KafkaLocalServer, MessageSender, MessageListener, RecordProcessorTrait }

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.Joined

import org.apache.kafka.clients.consumer.ConsumerRecord
import ImplicitConversions._

/**
  * End-to-end integration test that demonstrates how to perform a join between a KStream and a
  * KTable (think: KStream.leftJoin(KTable)), i.e. an example of a stateful computation.
  *
  * See StreamToTableJoinIntegrationTest for the equivalent Java example.
  *
  * Note: We intentionally use JUnit4 (wrapped by ScalaTest) for implementing this Scala integration
  * test so it is easier to compare this Scala code with the equivalent Java code at
  * StreamToTableJoinIntegrationTest.  One difference is that, to simplify the Scala/Junit integration, we
  * switched from BeforeClass (which must be `static`) to Before as well as from @ClassRule (which
  * must be `static` and `public`) to a workaround combination of `@Rule def` and a `private val`.
  */

object StreamToTableJoinScalaIntegrationTestImplicitSerdes extends TestSuite[KafkaLocalServer] with StreamToTableJoinTestData {

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
    implicit val stringSerde: Serde[String] = Serdes.String()
    implicit val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]

    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, s"stream-table-join-scala-integration-test-implicit-serdes-${scala.util.Random.nextInt(100)}")
      p.put(StreamsConfig.CLIENT_ID_CONFIG, "join-scala-integration-test-implicit-serdes-standard-consumer")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100")
      p.put(StreamsConfig.STATE_DIR_CONFIG, localStateDir)
      p
    }

    // this will make the leftJoin use the key and value serde from this implicit, while use the default
    // from config for otherValueSerde
    implicit val joined: Joined[String, Long, String] = Joined.keySerde(stringSerde).withValueSerde(longSerde)

    /**
     * Patterns for handling serdes in leftJoin (similar will be the handling of all join functions that accept a Joined argument)
     *
     * a. For `Joined[K, V, VO`], just make the implicit serdes available for `K`, `V` and `VO`. In the following
     *    example of `leftJoin`, we have `Joined[String, Long, String]` and have the implicits `stringSerde` and
     *    `longSerdes` available in scope. This should be enough to make an implicit `Joined` for `leftJoin`.
     *
     * b. Want to use default serdes from config for key and otherValue. Add the implicit `Joined` in scope as:
     *    `implicit val joined: Joined[String, Long, String] = Joined.valueSerde(longSerde)`. The other serdes will
     *    be picked up as `null` and used from the config.
     */ 

    val builder = new StreamsBuilderS()

    val userClicksStream: KStreamS[String, Long] = builder.stream(userClicksTopic)

    val userRegionsTable: KTableS[String, String] = builder.table(userRegionsTopic)

    // Compute the total per region by summing the individual click counts per region.
    val clicksPerRegion: KTableS[String, Long] = 
      userClicksStream

        // Join the stream against the table.
        .leftJoin(userRegionsTable, (clicks: Long, region: String) => (if (region == null) "UNKNOWN" else region, clicks))

        // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
        .map((_, regionWithClicks) => regionWithClicks)

        // Compute the total per region by summing the individual click counts per region.
        .groupByKey
        .reduce(_ + _)

    // Write the (continuously updating) results to the output topic.
    clicksPerRegion.toStream.to(outputTopic)

    val streams: KafkaStreams = new KafkaStreams(builder.build, streamsConfiguration)

    streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      override def uncaughtException(t: Thread, e: Throwable): Unit = try {
        println(s"Stream terminated because of uncaught exception .. Shutting down app", e)
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
    // To keep this code example simple and easier to understand/reason about, we publish all
    // user-region records before any user-click records (cf. step 3).  In practice though,
    // data records would typically be arriving concurrently in both input streams/topics.
    val sender1 = MessageSender[String, String](brokers, classOf[StringSerializer].getName, classOf[StringSerializer].getName) 
    userRegions.foreach(r => sender1.writeKeyValue(userRegionsTopic, r.key, r.value))

    //
    // Step 3: Publish some user click events.
    //
    val sender2 = MessageSender[String, Long](brokers, classOf[StringSerializer].getName, classOf[LongSerializer].getName) 
    userClicks.foreach(r => sender2.writeKeyValue(userClicksTopic, r.key, r.value))

    //
    // Step 4: Verify the application's output data.
    //
    val listener = MessageListener(brokers, outputTopic, "join-scala-integration-test-standard-consumer", 
      classOf[StringDeserializer].getName, 
      classOf[LongDeserializer].getName, 
      new RecordProcessor
    )

    val l = listener.waitUntilMinKeyValueRecordsReceived(expectedClicksPerRegion.size, 30000)
    streams.close()
    assertEquals(l.sortBy(_.key), expectedClicksPerRegion.sortBy(_.key))
  }

  class RecordProcessor extends RecordProcessorTrait[String, Long] {
    override def processRecord(record: ConsumerRecord[String, Long]): Unit = { 
      // println(s"Get Message $record")
    }
  }
}
