/**
  * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
  */
package com.lightbend.kafka.scala.server

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import java.util.Properties

object MessageSender {
  private val ACKS_CONFIG       = "all"  // Blocking on the full commit of the record
  private val RETRIES_CONFIG    = "1"    // Number of retries on put
  private val BATCH_SIZE_CONFIG = "1024" // Buffers for unsent records for each partition - controlls batching
  private val LINGER_MS_CONFIG  = "1"    // Timeout for more records to arive - controlls batching

  private val BUFFER_MEMORY_CONFIG = "1024000" // Controls the total amount of memory available to the producer for buffering.
  // If records are sent faster than they can be transmitted to the server then this
  // buffer space will be exhausted. When the buffer space is exhausted additional
  // send calls will block. The threshold for time to block is determined by max.block.ms
  // after which it throws a TimeoutException.

  def providerProperties(brokers: String, keySerializer: String, valueSerializer: String): Properties = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG)
    props.put(ProducerConfig.RETRIES_CONFIG, RETRIES_CONFIG)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE_CONFIG)
    props.put(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS_CONFIG)
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, BUFFER_MEMORY_CONFIG)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer)
    props
  }

  def apply[K, V](brokers: String, keySerializer: String, valueSerializer: String): MessageSender[K, V] =
    new MessageSender[K, V](brokers, keySerializer, valueSerializer)
}

class MessageSender[K, V](val brokers: String, val keySerializer: String, val valueSerializer: String) {

  import MessageSender._
  val producer = new KafkaProducer[K, V](providerProperties(brokers, keySerializer, valueSerializer))

  def writeKeyValue(topic: String, key: K, value: V): Unit = {
    val result = producer.send(new ProducerRecord[K, V](topic, key, value)).get
    producer.flush()
  }

  def writeValue(topic: String, value: V): Unit = {
    val result = producer.send(new ProducerRecord[K, V](topic, null.asInstanceOf[K], value)).get // scalastyle:ignore
    producer.flush()
  }

  def batchWriteValue(topic: String, batch: Seq[V]): Seq[RecordMetadata] = {
    val result = batch.map(value => producer.send(new ProducerRecord[K, V](topic, null.asInstanceOf[K], value)).get) // scalastyle:ignore
    producer.flush()
    result
  }

  def close(): Unit =
    producer.close()
}
