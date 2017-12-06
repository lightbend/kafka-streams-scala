package com.lightbend.kafka.scala.streams

import ImplicitConversions._
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.common.utils.Bytes

class KTableS[K, V](val inner: KTable[K, V]) {

  def filter(predicate: (K, V) => Boolean): KTableS[K, V] = {
    inner.filter(predicate(_, _))
  }

  def filter(predicate: (K, V) => Boolean,
    materialized: Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]]): KTableS[K, V] = {
    inner.filter(predicate(_, _), materialized)
  }

  def filterNot(predicate: (K, V) => Boolean): KTableS[K, V] = {
    inner.filterNot(predicate(_, _))
  }

  def filterNot(predicate: (K, V) => Boolean,
    materialized: Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]]): KTableS[K, V] = {
    inner.filterNot(predicate(_, _), materialized)
  }

  def mapValues[VR](mapper: V => VR): KTableS[K, VR] = {
    inner.mapValues[VR](mapper.asValueMapper)
  }

  def mapValues[VR](mapper: V => VR,
    materialized: Materialized[K, VR, KeyValueStore[Bytes, Array[Byte]]]): KTableS[K, VR] = {
    inner.mapValues[VR](mapper.asValueMapper, materialized)
  }

  def toStream: KStreamS[K, V] = inner.toStream

  def toStream[KR](mapper: (K, V) => KR): KStreamS[KR, V] = {
    inner.toStream[KR](mapper.asKeyValueMapper)
  }

  def groupBy[KR, VR](selector: (K, V) => (KR, VR)): KGroupedTableS[KR, VR] = {
    inner.groupBy(selector.asKeyValueMapper)
  }

  def groupBy[KR, VR](selector: (K, V) => (KR, VR),
    serialized: Serialized[KR, VR]): KGroupedTableS[KR, VR] = {

    inner.groupBy(selector.asKeyValueMapper, serialized)
  }

  def join[VO, VR](other: KTableS[K, VO],
    joiner: (V, VO) => VR): KTableS[K, VR] = {

    inner.join[VO, VR](other.inner, joiner.asValueJoiner)
  }

  def join[VO, VR](other: KTableS[K, VO],
    joiner: (V, VO) => VR,
    materialized: Materialized[K, VR, KeyValueStore[Bytes, Array[Byte]]]): KTableS[K, VR] = {

    val joinerJ: ValueJoiner[V, VO, VR] = (v1, v2) => joiner(v1, v2)
    inner.join[VO, VR](other.inner, joinerJ, materialized)
  }

  def leftJoin[VO, VR](other: KTableS[K, VO],
    joiner: (V, VO) => VR): KTableS[K, VR] = {

    val joinerJ: ValueJoiner[V, VO, VR] = (v1, v2) => joiner(v1, v2)
    inner.leftJoin[VO, VR](other.inner, joinerJ)
  }

  def leftJoin[VO, VR](other: KTableS[K, VO],
    joiner: (V, VO) => VR,
    materialized: Materialized[K, VR, KeyValueStore[Bytes, Array[Byte]]]): KTableS[K, VR] = {

    val joinerJ: ValueJoiner[V, VO, VR] = (v1, v2) => joiner(v1, v2)
    inner.leftJoin[VO, VR](other.inner, joinerJ, materialized)
  }

  def outerJoin[VO, VR](other: KTableS[K, VO],
    joiner: (V, VO) => VR): KTableS[K, VR] = {

    val joinerJ: ValueJoiner[V, VO, VR] = (v1, v2) => joiner(v1, v2)
    inner.outerJoin[VO, VR](other.inner, joinerJ)
  }

  def outerJoin[VO, VR](other: KTableS[K, VO],
    joiner: (V, VO) => VR,
    materialized: Materialized[K, VR, KeyValueStore[Bytes, Array[Byte]]]): KTableS[K, VR] = {

    inner.outerJoin[VO, VR](other.inner, joiner.asValueJoiner, materialized)
  }

  def queryableStoreName: String =
    inner.queryableStoreName
}
