package com.lightbend.kafka.scala.server

import org.apache.kafka.clients.consumer.ConsumerRecord

// A trait, that should be implemented by any listener implementation

trait RecordProcessorTrait[K, V] {

  def processRecord(record: ConsumerRecord[K, V]): Unit
}