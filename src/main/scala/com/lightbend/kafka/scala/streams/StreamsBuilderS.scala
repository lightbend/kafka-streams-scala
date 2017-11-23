package com.lightbend.kafka.scala.streams

import java.util.regex.Pattern

import ImplicitConversions._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.processor.{ProcessorSupplier, StateStore}
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}

import scala.collection.JavaConverters._

class StreamsBuilderS {

  val inner = new StreamsBuilder

  def stream[K, V](topics: String*) : KStreamS[K, V] = {
     inner.stream[K, V](topics.asJava)
  }

  def stream[K, V](topics: List[String], consumed: Consumed[K, V]) : KStreamS[K, V] = {
     inner.stream[K, V](topics.asJava, consumed)
  }

  def stream[K, V](offsetReset: Topology.AutoOffsetReset,
                   topics: String*) : KStreamS[K, V] =
    inner.stream[K, V](topics.asJava, Consumed.`with`[K,V](offsetReset))

  def stream[K, V](topicPattern: Pattern) : KStreamS[K, V] =
    inner.stream[K, V](topicPattern)

  def stream[K, V](offsetReset: Topology.AutoOffsetReset, topicPattern: Pattern): KStreamS[K, V] =
    inner.stream[K, V](topicPattern, Consumed.`with`[K,V](offsetReset))

  def table[K, V](topic: String) : KTableS[K, V] = inner.table[K, V](topic)

  def table[K, V](offsetReset: Topology.AutoOffsetReset,
                  topic: String) : KTableS[K, V] =
    inner.table[K, V](topic,  Consumed.`with`[K,V](offsetReset))


  def globalTable[K, V](topic: String): GlobalKTable[K, V] =
    inner.globalTable(topic)

  def addStateStore(builder: StoreBuilder[_ <: StateStore]): StreamsBuilder = inner.addStateStore(builder)

  def addGlobalStore(storeBuilder: StoreBuilder[_ <: StateStore], topic: String, sourceName: String, consumed: Consumed[_, _], processorName: String, stateUpdateSupplier: ProcessorSupplier[_, _]): StreamsBuilder =
    inner.addGlobalStore(storeBuilder,topic,sourceName,consumed,processorName,stateUpdateSupplier)


  def build() : Topology = inner.build()
}


