/**
  * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
  * Copyright 2017-2018 Alexis Seigneurin.
  */
package com.lightbend.kafka.scala.streams

import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.WindowStore
import org.apache.kafka.common.utils.Bytes
import ImplicitConversions._
import FunctionConversions._

/**
  * Wraps the Java class TimeWindowedKStream and delegates method calls to the underlying Java object.
  */
class TimeWindowedKStreamS[K, V](val inner: TimeWindowedKStream[K, V]) {

  def aggregate[VR](initializer: () => VR, aggregator: (K, V, VR) => VR): KTableS[Windowed[K], VR] =
    inner.aggregate(initializer.asInitializer, aggregator.asAggregator)

  def aggregate[VR](initializer: () => VR,
                    aggregator: (K, V, VR) => VR,
                    materialized: Materialized[K, VR, WindowStore[Bytes, Array[Byte]]]): KTableS[Windowed[K], VR] =
    inner.aggregate(initializer.asInitializer, aggregator.asAggregator, materialized)

  def count(): KTableS[Windowed[K], Long] = inner.count().asInstanceOf[KTable[Windowed[K], Long]]

  def count(
    materialized: Materialized[Windowed[K], Long, WindowStore[Bytes, Array[Byte]]]
  ): KTableS[Windowed[K], Long] =
    inner.count(materialized)

  def reduce(reducer: (V, V) => V): KTableS[Windowed[K], V] =
    inner.reduce(reducer.asReducer)

  def reduce(reducer: (V, V) => V,
             materialized: Materialized[K, V, WindowStore[Bytes, Array[Byte]]]): KTableS[Windowed[K], V] =
    inner.reduce(reducer.asReducer, materialized)
}
