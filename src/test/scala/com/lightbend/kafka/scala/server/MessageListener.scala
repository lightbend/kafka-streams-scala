/**
  * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
  */
package com.lightbend.kafka.scala.server

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.streams.KeyValue
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object MessageListener {
  private val AUTO_COMMIT_INTERVAL_MS_CONFIG = "1000"  // Frequency of offset commits
  private val SESSION_TIMEOUT_MS_CONFIG      = "30000" // The timeout used to detect failures - should be greater then processing time
  private val MAX_POLL_RECORDS_CONFIG        = "50"    // Max number of records consumed in a single poll

  def consumerProperties(brokers: String,
                         group: String,
                         keyDeserializer: String,
                         valueDeserializer: String): Map[String, AnyRef] =
    Map[String, AnyRef](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG                 -> group,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG       -> "true",
      ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG  -> AUTO_COMMIT_INTERVAL_MS_CONFIG,
      ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG       -> SESSION_TIMEOUT_MS_CONFIG,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG         -> MAX_POLL_RECORDS_CONFIG,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "latest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> keyDeserializer,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> valueDeserializer
    )

  def apply[K, V](brokers: String,
                  topic: String,
                  group: String,
                  keyDeserializer: String,
                  valueDeserializer: String,
                  processor: RecordProcessorTrait[K, V]): MessageListener[K, V] =
    new MessageListener[K, V](brokers, topic, group, keyDeserializer, valueDeserializer, processor)
}

class MessageListener[K, V](brokers: String,
                            topic: String,
                            group: String,
                            keyDeserializer: String,
                            valueDeserializer: String,
                            processor: RecordProcessorTrait[K, V]) {

  import MessageListener._

  def readKeyValues(maxMessages: Int): List[KeyValue[K, V]] = {
    val pollIntervalMs     = 100
    val maxTotalPollTimeMs = 2000
    var totalPollTimeMs    = 0

    val consumer =
      new KafkaConsumer[K, V](consumerProperties(brokers, group, keyDeserializer, valueDeserializer).asJava)
    consumer.subscribe(Seq(topic).asJava)

    val consumedValues = ListBuffer.empty[KeyValue[K, V]]

    while (totalPollTimeMs < maxTotalPollTimeMs && continueConsuming(consumedValues.size, maxMessages)) {
      totalPollTimeMs = totalPollTimeMs + pollIntervalMs
      val records = consumer.poll(pollIntervalMs)
      records.asScala.foreach { record =>
        processor.processRecord(record)
        consumedValues += new KeyValue(record.key, record.value)
      }
    }
    consumer.close()
    consumedValues.toList
  }

  def continueConsuming(messagesConsumed: Int, maxMessages: Int): Boolean =
    maxMessages <= 0 || messagesConsumed < maxMessages

  def waitUntilMinKeyValueRecordsReceived(
    expectedNumRecords: Int,
    waitTime: Long,
    startTime: Long = System.currentTimeMillis(),
    accumData: ListBuffer[KeyValue[K, V]] = ListBuffer.empty[KeyValue[K, V]]
  ): List[KeyValue[K, V]] = {

    val readData = readKeyValues(-1)
    accumData ++= readData

    if (accumData.size >= expectedNumRecords) accumData.toList
    else if (System.currentTimeMillis() > startTime + waitTime)
      throw new AssertionError(
        s"Expected $expectedNumRecords but received only ${accumData.size} records before timeout $waitTime ms"
      )
    else {
      Thread.sleep(Math.min(waitTime, 1000L))
      waitUntilMinKeyValueRecordsReceived(expectedNumRecords, waitTime, startTime, accumData)
    }
  }
}
