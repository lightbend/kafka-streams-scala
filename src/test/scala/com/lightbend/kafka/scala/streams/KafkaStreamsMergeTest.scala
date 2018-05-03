/**
  * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
  */
package com.lightbend.kafka.scala.streams

import java.util.Properties
import java.util.regex.Pattern

import com.lightbend.kafka.scala.server.{KafkaLocalServer, MessageListener, MessageSender, RecordProcessorTrait}
import minitest.TestSuite
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import ImplicitConversions._
import com.typesafe.scalalogging.LazyLogging

object KafkaStreamsMergeTest extends TestSuite[KafkaLocalServer] with WordCountMergeTestData with LazyLogging {

  override def setup(): KafkaLocalServer = {
    val s = KafkaLocalServer(cleanOnStart = true, Some(localStateDir))
    s.start()
    s
  }

  override def tearDown(server: KafkaLocalServer): Unit =
    server.stop()

  test("should count words") { server =>
    server.createTopic(inputTopic1)
    server.createTopic(inputTopic2)
    server.createTopic(outputTopic)

    //
    // Step 1: Configure and start the processor topology.
    //
    import DefaultSerdes._

    val streamsConfiguration = new Properties()
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, s"wordcount-${scala.util.Random.nextInt(100)}")
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcountgroup")

    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, localStateDir)

    val builder = new StreamsBuilderS()

    val textLines1 = builder.stream[String, String](inputTopic1)
    val textLines2 = builder.stream[String, String](inputTopic2)

    val textLines = textLines1.merge(textLines2)

    val pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)

    val wordCounts: KTableS[String, Long] =
      textLines
        .flatMapValues(v => pattern.split(v.toLowerCase))
        .groupBy((k, v) => v)
        .count()

    wordCounts.toStream.to(outputTopic)

    val streams = new KafkaStreams(builder.build(), streamsConfiguration)
    streams.start()

    //
    // Step 2: Produce some input data to the input topics.
    //
    val sender =
      MessageSender[String, String](brokers, classOf[StringSerializer].getName, classOf[StringSerializer].getName)
    val mvals1 = sender.batchWriteValue(inputTopic1, inputValues)
    val mvals2 = sender.batchWriteValue(inputTopic2, inputValues)

    //
    // Step 3: Verify the application's output data.
    //
    val listener = MessageListener(brokers,
                                   outputTopic,
                                   "wordcountgroup",
                                   classOf[StringDeserializer].getName,
                                   classOf[LongDeserializer].getName,
                                   new RecordProcessor)

    val l = listener.waitUntilMinKeyValueRecordsReceived(expectedWordCounts.size, 30000)

    assertEquals(l.sortBy(_.key), expectedWordCounts.sortBy(_.key))

    streams.close()
  }

  class RecordProcessor extends RecordProcessorTrait[String, Long] {
    override def processRecord(record: ConsumerRecord[String, Long]): Unit = {
      // logger.info(s"Get Message $record")
    }
  }

}

trait WordCountMergeTestData {
  val inputTopic1   = s"inputTopic1.${scala.util.Random.nextInt(100)}"
  val inputTopic2   = s"inputTopic2.${scala.util.Random.nextInt(100)}"
  val outputTopic   = s"outputTpic.${scala.util.Random.nextInt(100)}"
  val brokers       = "localhost:9092"
  val localStateDir = "local_state_data"

  val inputValues = List(
    "Hello Kafka Streams",
    "All streams lead to Kafka",
    "Join Kafka Summit",
    "И теперь пошли русские слова"
  )

  val expectedWordCounts: List[KeyValue[String, Long]] = List(
    new KeyValue("hello", 2L),
    new KeyValue("all", 2L),
    new KeyValue("streams", 4L),
    new KeyValue("lead", 2L),
    new KeyValue("to", 2L),
    new KeyValue("join", 2L),
    new KeyValue("kafka", 6L),
    new KeyValue("summit", 2L),
    new KeyValue("и", 2L),
    new KeyValue("теперь", 2L),
    new KeyValue("пошли", 2L),
    new KeyValue("русские", 2L),
    new KeyValue("слова", 2L)
  )
}
