/**
  * Copyright (C) 2018 OpenShine SL <https://www.openshine.com>
  * Copyright (C) 2018  <https://www.lightbend.com>
  * Adapted from the parent package
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
import com.lightbend.kafka.scala.streams.algebird.{CMSStore, CMSStoreBuilder}
import minitest.TestSuite
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder, StreamsConfig}
import com.lightbend.kafka.scala.streams.ImplicitConversions.Tuple2ToKeyValue

/**
  * End-to-end integration test that demonstrates how to probabilistically count items in an input stream.
  *
  * This example uses a custom state store implementation, [[CMSStore]], that is backed by a
  * Count-Min Sketch data structure.
  */
trait ProbabilisticCountingScalaIntegrationTestData {
  val brokers = "localhost:9092"
  val inputTopic = s"inputTopic.${scala.util.Random.nextInt(100)}"
  val outputTopic = s"output-topic.${scala.util.Random.nextInt(100)}"
  val localStateDir = "local_state_data"

  val inputTextLines: Seq[String] = Seq(
    "Hello Kafka Streams",
    "All streams lead to Kafka",
    "Join Kafka Summit"
  )

  val expectedWordCounts: Seq[KeyValue[String, Long]] = Seq(
    ("hello", 1L),
    ("kafka", 1L),
    ("streams", 1L),
    ("all", 1L),
    ("streams", 2L),
    ("lead", 1L),
    ("to", 1L),
    ("kafka", 2L),
    ("join", 1L),
    ("kafka", 3L),
    ("summit", 1L)
  )
}

object ProbabilisticCountingScalaIntegrationTest extends TestSuite[KafkaLocalServer]
  with ProbabilisticCountingScalaIntegrationTestData {

  override def setup(): KafkaLocalServer = {
    val s = KafkaLocalServer(true, Some(localStateDir))
    s.start()
    s
  }

  override def tearDown(server: KafkaLocalServer): Unit = {
    server.stop()
  }

  test("shouldProbabilisticallyCountWords") { server =>

    server.createTopic(inputTopic)
    server.createTopic(outputTopic)

    //
    // Step 1: Configure and start the processor topology.
    //
    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, s"probabilistic-counting-scala-integration-test-${scala.util.Random.nextInt(100)}")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
      p.put(StreamsConfig.STATE_DIR_CONFIG, localStateDir)
      p
    }

    import SerdeDerivations._
    implicit val builder = new StreamsBuilder()

    val cmsStoreName = "cms-store"
    val cmsStoreBuilder = {
      val changeloggingEnabled = true
      val changelogConfig: java.util.HashMap[String, String] = {
        val cfg = new java.util.HashMap[String, String]
        val segmentSizeBytes = (20 * 1024 * 1024).toString
        cfg.put("segment.bytes", segmentSizeBytes)
        cfg
      }
      new CMSStoreBuilder[String](cmsStoreName, Serdes.String())
        .withLoggingEnabled(changelogConfig)
    }
    builder.addStateStore(cmsStoreBuilder)

    class ProbabilisticCounter extends Transformer[Array[Byte], String, (String, Long)] {

      private var cmsState: CMSStore[String] = _
      private var processorContext: ProcessorContext = _

      override def init(processorContext: ProcessorContext): Unit = {
        this.processorContext = processorContext
        cmsState = this.processorContext.getStateStore(cmsStoreName).asInstanceOf[CMSStore[String]]
      }

      override def transform(key: Array[Byte], value: String): (String, Long) = {
        // Count the record value, think: "+ 1"
        cmsState.put(value, this.processorContext.timestamp())

        // In this example: emit the latest count estimate for the record value.  We could also do
        // something different, e.g. periodically output the latest heavy hitters via `punctuate`.
        (value, cmsState.get(value))
      }

      override def punctuate(l: Long): (String, Long) = null

      override def close(): Unit = {}
    }

    // Read the input from Kafka.
    val textLines: TSKStream[Array[Byte], String] = TSKStream[Array[Byte], String](inputTopic)

    textLines
      .flatMapValues(value => value.toLowerCase.split("\\W+").toIterable)
      .transform(new ProbabilisticCounter, cmsStoreName)
      .to(outputTopic)

    val streams: KafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration)
    streams.start()

    //
    // Step 2: Publish some input text lines.
    //
    val sender = MessageSender[String, String](brokers, classOf[StringSerializer].getName, classOf[StringSerializer].getName)
    val mvals = sender.batchWriteValue(inputTopic, inputTextLines)

    //
    // Step 3: Verify the application's output data.
    //
    val listener = MessageListener(brokers, outputTopic, "probwordcountgroup",
      classOf[StringDeserializer].getName,
      classOf[LongDeserializer].getName,
      new RecordProcessor
    )

    val l = listener.waitUntilMinKeyValueRecordsReceived(expectedWordCounts.size, 30000)

    assertEquals(l.sortBy(_.key), expectedWordCounts.sortBy(_.key))
    streams.close()
  }

  class RecordProcessor extends RecordProcessorTrait[String, Long] {
    override def processRecord(record: ConsumerRecord[String, Long]): Unit = {
      // println(s"Get Message $record")
    }
  }

}
