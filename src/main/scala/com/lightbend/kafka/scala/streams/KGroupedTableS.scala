/**
  * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
  * Copyright 2017-2018 Alexis Seigneurin.
  */
package com.lightbend.kafka.scala.streams

import ImplicitConversions._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.common.utils.Bytes
import FunctionConversions._

/**
  * Wraps the Java class KGroupedTable and delegates method calls to the underlying Java object.
  */
class KGroupedTableS[K, V](inner: KGroupedTable[K, V]) {

  type ByteArrayKVStore = KeyValueStore[Bytes, Array[Byte]]

  def count(): KTableS[K, Long] = {
    val c: KTableS[K, java.lang.Long] = inner.count()
    c.mapValues[Long](Long2long(_))
  }

  def count(materialized: Materialized[K, Long, ByteArrayKVStore]): KTableS[K, Long] =
    inner.count(materialized)

  def reduce(adder: (V, V) => V, subTractor: (V, V) => V): KTableS[K, V] =
    // need this explicit asReducer for Scala 2.11 or else the SAM conversion doesn't take place
    // works perfectly with Scala 2.12 though
    inner.reduce(((v1, v2) => adder(v1, v2)).asReducer, ((v1, v2) => subTractor(v1, v2)).asReducer)

  def reduce(adder: (V, V) => V,
             subtractor: (V, V) => V,
             materialized: Materialized[K, V, ByteArrayKVStore]): KTableS[K, V] =
    // need this explicit asReducer for Scala 2.11 or else the SAM conversion doesn't take place
    // works perfectly with Scala 2.12 though
    inner.reduce(((v1, v2) => adder(v1, v2)).asReducer, ((v1, v2) => subtractor(v1, v2)).asReducer, materialized)

  def aggregate[VR](initializer: () => VR, adder: (K, V, VR) => VR, subtractor: (K, V, VR) => VR): KTableS[K, VR] =
    inner.aggregate(initializer.asInitializer, adder.asAggregator, subtractor.asAggregator)

  def aggregate[VR](initializer: () => VR,
                    adder: (K, V, VR) => VR,
                    subtractor: (K, V, VR) => VR,
                    materialized: Materialized[K, VR, ByteArrayKVStore]): KTableS[K, VR] =
    inner.aggregate(initializer.asInitializer, adder.asAggregator, subtractor.asAggregator, materialized)
}
