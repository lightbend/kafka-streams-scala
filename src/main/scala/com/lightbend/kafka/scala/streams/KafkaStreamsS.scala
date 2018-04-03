package com.lightbend.kafka.scala.streams

import java.util.Properties

import org.apache.kafka.common.{Metric, MetricName}
import org.apache.kafka.streams.processor.{StateRestoreListener, StreamPartitioner, ThreadMetadata}
import org.apache.kafka.streams.state.{QueryableStoreType, StreamsMetadata}
import org.apache.kafka.streams.{KafkaClientSupplier, KafkaStreams, StreamsConfig, Topology}

import scala.collection.JavaConverters._

class KafkaStreamsS(inner: KafkaStreams) {

  def allMetadata(): Iterable[StreamsMetadata] = {
    inner.allMetadata().asScala
  }

  def allMetadataForStore(storeName: String): Iterable[StreamsMetadata] = {
    inner.allMetadataForStore(storeName).asScala
  }

  def cleanUp() = {
    inner.cleanUp()
    this
  }

  def close() = {
    inner.close()
  }

  def close(timeout: Long, timeUnit: java.util.concurrent.TimeUnit) = {
    inner.close(timeout, timeUnit)
  }

  def localThreadsMetadata(): Set[ThreadMetadata] = {
    inner.localThreadsMetadata.asScala.toSet
  }

  def metadataForKey[K](storeName: String, key: K, keySerializer: Serializer[K]): StreamsMetadata = {
    inner.metadataForKey(storeName, key, keySerializer)
  }

  def metadataForKey[K](storeName: String, key: K, partitioner: StreamPartitioner[_ >: K, _]): StreamsMetadata = {
    inner.metadataForKey(storeName, key, partitioner)
  }

  def metrics(): Map[MetricName, _ <: Metric] = {
    inner.metrics().asScala.toMap
  }

  def withGlobalStateRestoreListener(globalStateRestoreListener: StateRestoreListener) = {
    inner.setGlobalStateRestoreListener(globalStateRestoreListener)
    this
  }

  def withStateListener(listener: KafkaStreams.StateListener) = {
    inner.setStateListener(listener)
    this
  }

  def withUncaughtExceptionHandler(eh: java.lang.Thread.UncaughtExceptionHandler) = {
    inner.setUncaughtExceptionHandler(eh)
    this
  }

  def start(): KafkaStreamsS = {
    inner.start()
    this
  }

  def state(): KafkaStreams.State = {
    inner.state()
  }

  def store[T](storeName: String, queryableStoreType: QueryableStoreType[T]) = {
    inner.store(storeName, queryableStoreType)
  }
}

object KafkaStreamsS {
  def apply(s: StreamsBuilderS, p: Properties): KafkaStreamsS = new KafkaStreamsS(new KafkaStreams(s.build(), p))

  def apply(topology: Topology, p: Properties): KafkaStreamsS = new KafkaStreamsS(new KafkaStreams(topology, p))

  def apply(topology: Topology, config: StreamsConfig) = new KafkaStreamsS(new KafkaStreams(topology, config))

  def apply(topology: Topology, config: StreamsConfig, clientSupplier: KafkaClientSupplier) = new KafkaStreamsS(new KafkaStreams(topology, config, clientSupplier))

}
