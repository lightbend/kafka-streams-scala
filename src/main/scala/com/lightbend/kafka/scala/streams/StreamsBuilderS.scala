/**
  * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
  */

package com.lightbend.kafka.scala.streams

import java.util.regex.Pattern

import com.lightbend.kafka.scala.streams.ImplicitConversions._
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{GlobalKTable, Materialized}
import org.apache.kafka.streams.processor.{ProcessorSupplier, StateStore}
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder}
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}

import scala.collection.JavaConverters._

/**
  * Wraps the Java class StreamsBuilder and delegates method calls to the underlying Java object.
  */
class StreamsBuilderS(inner: StreamsBuilder = new StreamsBuilder) {

  def stream[K, V](topic: String)(implicit consumed: Perhaps[Consumed[K, V]]): KStreamS[K, V] =
    consumed.fold[KStreamS[K, V]] { inner.stream[K, V](topic) } { implicit ev => inner.stream[K, V](topic, ev) }

  def stream[K, V](topics: List[String])(implicit consumed: Perhaps[Consumed[K, V]]): KStreamS[K, V] =
    consumed.fold[KStreamS[K, V]] { inner.stream[K, V](topics.asJava) } { implicit ev => inner.stream[K, V](topics.asJava, ev) }

  def stream[K, V](topicPattern: Pattern)(implicit consumed: Perhaps[Consumed[K, V]]): KStreamS[K, V] =
    consumed.fold[KStreamS[K, V]] { inner.stream[K, V](topicPattern) } { implicit ev => inner.stream[K, V](topicPattern, ev) }

  def table[K, V](topic: String)(implicit consumed: Perhaps[Consumed[K, V]]): KTableS[K, V] = 
    consumed.fold[KTableS[K, V]] { inner.table[K, V](topic) } { implicit ev => inner.table[K, V](topic, ev) }

  def table[K, V](topic: String, materialized: Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]])
    (implicit consumed: Perhaps[Consumed[K, V]]): KTableS[K, V] =
    consumed.fold[KTableS[K, V]] { inner.table(topic, materialized) } { implicit ev => inner.table[K, V](topic, ev, materialized) }

  def globalTable[K, V](topic: String)(implicit consumed: Perhaps[Consumed[K, V]]): GlobalKTable[K, V] =
    consumed.fold[GlobalKTable[K, V]] { inner.globalTable(topic) } { implicit ev => inner.globalTable(topic, ev) }

  def globalTable[K, V](topic: String, materialized: Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]])
    (implicit consumed: Perhaps[Consumed[K, V]]): GlobalKTable[K, V] =
    consumed.fold[GlobalKTable[K, V]] { inner.globalTable(topic, materialized) } { implicit ev => inner.globalTable(topic, ev, materialized) }

  def addStateStore(builder: StoreBuilder[_ <: StateStore]): StreamsBuilder = inner.addStateStore(builder)

  def addGlobalStore(storeBuilder: StoreBuilder[_ <: StateStore], topic: String, sourceName: String, consumed: Consumed[_, _], processorName: String, stateUpdateSupplier: ProcessorSupplier[_, _]): StreamsBuilder =
    inner.addGlobalStore(storeBuilder, topic, sourceName, consumed, processorName, stateUpdateSupplier)

  def build(): Topology = inner.build()
}
