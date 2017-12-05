package com.lightbend.kafka.scala.iq.example
package processor

import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.internals.{ProcessorStateManager, RecordCollector, ProcessorContextImpl}
import org.apache.kafka.streams.state.StateSerdes

class BFStoreChangeLogger[K, V](val storeName: String,
                                val context: ProcessorContext,
                                val partition: Int,
                                val serialization: StateSerdes[K, V]) {

  private val topic = ProcessorStateManager.storeChangelogTopic(context.applicationId, storeName)
  private val collector = context.asInstanceOf[RecordCollector.Supplier].recordCollector

  def this(storeName: String, context: ProcessorContext, serialization: StateSerdes[K, V]) {
    this(storeName, context, context.taskId.partition, serialization)
  }

  def logChange(key: K, value: V): Unit = {
    if (collector != null) {
      val keySerializer = serialization.keySerializer
      val valueSerializer = serialization.valueSerializer
      collector.send(this.topic, key, value, this.partition, context.timestamp, keySerializer, valueSerializer)
    }
  }
}

