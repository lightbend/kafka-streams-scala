package com.lightbend.kafka.scala.server

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata }
import java.util.Properties

object MessageSender {
  private val ACKCONFIGURATION = "all" // Blocking on the full commit of the record
  private val RETRYCOUNT = "1" // Number of retries on put
  private val BATCHSIZE = "1024" // Buffers for unsent records for each partition - controlls batching
  private val LINGERTIME = "1" // Timeout for more records to arive - controlls batching
  private val BUFFERMEMORY = "1024000" // Controls the total amount of memory available to the producer for buffering. If records are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is exhausted additional send calls will block. The threshold for time to block is determined by max.block.ms after which it throws a TimeoutException.

  def providerProperties(brokers: String, keySerializer: String, valueSerializer: String): Properties = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.ACKS_CONFIG, ACKCONFIGURATION)
    props.put(ProducerConfig.RETRIES_CONFIG, RETRYCOUNT)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCHSIZE)
    props.put(ProducerConfig.LINGER_MS_CONFIG, LINGERTIME)
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, BUFFERMEMORY)
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
    val result = producer.send(new ProducerRecord[K, V](topic, null.asInstanceOf[K], value)).get
    producer.flush()
  }

  def batchWriteValue(topic: String, batch: Seq[V]): Seq[RecordMetadata] = {
    val result = batch.map(value => {
      producer.send(new ProducerRecord[K, V](topic, null.asInstanceOf[K], value)).get})
    producer.flush()
    result
  }

  def close(): Unit = {
    producer.close()
  }
}
