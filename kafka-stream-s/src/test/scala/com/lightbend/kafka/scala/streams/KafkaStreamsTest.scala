package com.lightbend.kafka.scala.streams

import minitest.TestSuite
import com.lightbend.kafka.scala.server.{ KafkaLocalServer, MessageSender, MessageListener, RecordProcessorTrait }

import java.util.{ Properties, Locale }
import java.util.regex.Pattern

import org.apache.kafka.streams.{ KeyValue, StreamsConfig, KafkaStreams, Consumed }
import org.apache.kafka.streams.kstream.{ Materialized, Produced, KeyValueMapper, Printed }
import org.apache.kafka.common.serialization.{ Serdes, StringSerializer, StringDeserializer, Serde, LongDeserializer }
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.duration._

import ImplicitConversions._

object KafkaStreamsTest extends TestSuite[KafkaLocalServer] with WordCountTestData {

  override def setup(): KafkaLocalServer = {
    val s = KafkaLocalServer(true, Some(localStateDir))
    s.start()
    s
  }

  override def tearDown(server: KafkaLocalServer): Unit = {
    server.stop()
  }

  test("should count words") { server =>

    server.createTopic(inputTopic)
    server.createTopic(outputTopic)

    //
    // Step 1: Configure and start the processor topology.
    //
    val stringSerde = Serdes.String()
    val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]

    val streamsConfiguration = new Properties()
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, s"wordcount-${scala.util.Random.nextInt(100)}")
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcountgroup")
    
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, localStateDir)

    val builder = new StreamsBuilderS

    val textLines = builder.stream[String, String](inputTopic)

    val pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)

    val wordCounts: KTableS[String, Long] = 
      textLines.flatMapValues(v => pattern.split(v.toLowerCase))
        .groupBy((k, v) => v)
        .count()

    wordCounts.toStream.to(outputTopic, Produced.`with`(stringSerde, longSerde))

    val streams = new KafkaStreams(builder.build, streamsConfiguration)
    streams.start()

    //
    // Step 2: Produce some input data to the input topic.
    //
    val sender = MessageSender[String, String](brokers, classOf[StringSerializer].getName, classOf[StringSerializer].getName) 
    val mvals = sender.batchWriteValue(inputTopic, inputValues)

    //
    // Step 3: Verify the application's output data.
    //
    val listener = MessageListener(brokers, outputTopic, "wordcountgroup", 
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

trait WordCountTestData {
  val inputTopic = s"inputTopic.${scala.util.Random.nextInt(100)}"
  val outputTopic = s"outputTopic.${scala.util.Random.nextInt(100)}"
  val brokers = "localhost:9092"
  val localStateDir = "local_state_data"

  val inputValues = List(
    "Hello Kafka Streams",
    "All streams lead to Kafka",
    "Join Kafka Summit",
    "И теперь пошли русские слова"
  )

  val expectedWordCounts: List[KeyValue[String, Long]] = List(
    new KeyValue("hello", 1L),
    new KeyValue("all", 1L),
    new KeyValue("streams", 2L),
    new KeyValue("lead", 1L),
    new KeyValue("to", 1L),
    new KeyValue("join", 1L),
    new KeyValue("kafka", 3L),
    new KeyValue("summit", 1L),
    new KeyValue("и", 1L),
    new KeyValue("теперь", 1L),
    new KeyValue("пошли", 1L),
    new KeyValue("русские", 1L),
    new KeyValue("слова", 1L)
  )
}

