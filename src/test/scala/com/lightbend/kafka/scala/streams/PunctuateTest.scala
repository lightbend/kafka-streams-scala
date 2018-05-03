/**
  * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
  */
package com.lightbend.kafka.scala.streams

import java.util.Properties

import com.lightbend.kafka.scala.server.{KafkaLocalServer, MessageSender}
import com.typesafe.scalalogging.LazyLogging
import minitest.TestSuite
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext, PunctuationType}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

/**
  * This sample is using usage of punctuate, which is significantly changed in version 1.0 and
  * Kafka Streams Processor APIs (https://kafka.apache.org/10/documentation/streams/developer-guide/processor-api.html)
  * This code is based on the article "Problems With Kafka Streams:
  * The Saga Continues" (https://dzone.com/articles/problems-with-kafka-streams-the-saga-continues)
  */
object PunctuateTest extends TestSuite[KafkaLocalServer] with PunctuateTestData with LazyLogging {

  override def setup(): KafkaLocalServer = {
    val s = KafkaLocalServer(cleanOnStart = true, Some(localStateDir))
    s.start()
    s
  }

  override def tearDown(server: KafkaLocalServer): Unit =
    server.stop()

  test("should punctuate execution") { server =>
    server.createTopic(inputTopic)

    //
    // Step 1: Configure and start the processor topology.
    //

    val streamsConfiguration = new Properties()
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, s"punctuate-${scala.util.Random.nextInt(100)}")
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "punctuategroup")

    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())

    val topology = new Topology
    // Data input streams
    topology.addSource("data", inputTopic)
    // Processors
    topology.addProcessor("data processor", () => new SampleProcessor(5000), "data")
    val streams = new KafkaStreams(topology, streamsConfiguration)
    streams.start()
    // Allpw time for the streams to start up
    Thread.sleep(5000L)

    //
    // Step 2: Produce some input data to the input topic.
    //
    val sender =
      MessageSender[String, String](brokers, classOf[StringSerializer].getName, classOf[StringSerializer].getName)
    for (i <- 0 to 15) {
      sender.writeValue(inputTopic, i.toString)
      Thread.sleep(1000L) // sleep for 1 sec
    }

    // End test
    Thread.sleep(5000L) // sleep for 10 sec
    streams.close()
  }

  class SampleProcessor(punctuateTime: Long) extends AbstractProcessor[String, String] {

    var ctx: ProcessorContext = _
    var message               = ""

    override def init(context: ProcessorContext): Unit = {
      ctx = context
      ctx.schedule(punctuateTime,
                   PunctuationType.STREAM_TIME,
                   (timestamp: Long) => logger.info(s"Punctuator called at $timestamp, current message $message"))
    }

    override def process(key: String, value: String): Unit = {
      logger.info(s"Processing new message $value")
      message = value
    }
  }
}

trait PunctuateTestData {
  val inputTopic    = s"inputTopic.${scala.util.Random.nextInt(100)}"
  val outputTopic   = s"outputTopic.${scala.util.Random.nextInt(100)}"
  val brokers       = "localhost:9092"
  val localStateDir = "local_state_data"
}
