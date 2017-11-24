package com.lightbend.kafka.scala.streams

import ImplicitConversions._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.StreamPartitioner
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.common.utils.Bytes

class KTableS[K, V](val inner: KTable[K, V]) {

  def filter[SK >: K, SV >: V](predicate: (SK, SV) => Boolean): KTableS[K, V] = {
    val predicateJ: Predicate[SK, SV] = new Predicate[SK, SV]{
      override def test(k: SK, v: SV): Boolean = predicate(k, v)
    }
    inner.filter(predicateJ)
  }

  def filter[SK >: K, SV >: V](predicate: (SK, SV) => Boolean,
    materialized: Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]]): KTableS[K, V] = {
    val predicateJ: Predicate[SK, SV] = new Predicate[SK, SV]{
      override def test(k: SK, v: SV): Boolean = predicate(k, v)
    }
    inner.filter(predicateJ, materialized)
  }

  def filterNot[SK >: K, SV >: V](predicate: (SK, SV) => Boolean): KTableS[K, V] = {
    val predicateJ: Predicate[SK, SV] = new Predicate[SK, SV]{
      override def test(k: SK, v: SV): Boolean = predicate(k, v)
    }
    inner.filterNot(predicateJ)
  }

  def filterNot[SK >: K, SV >: V](predicate: (SK, SV) => Boolean,
    materialized: Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]]): KTableS[K, V] = {
    val predicateJ: Predicate[SK, SV] = new Predicate[SK, SV]{
      override def test(k: SK, v: SV): Boolean = predicate(k, v)
    }
    inner.filterNot(predicateJ, materialized)
  }

  def mapValues[VR, SV >: V, EVR <: VR](mapper: SV => EVR): KTableS[K, VR] = {
    def mapperJ: ValueMapper[SV, EVR] = (v) => mapper(v)
    inner.mapValues[VR](mapperJ)
  }

  def mapValues[VR, SV >: V, EVR <: VR](mapper: (SV) => EVR,
    materialized: Materialized[K, VR, KeyValueStore[Bytes, Array[Byte]]]): KTableS[K, VR] = {
    def mapperJ: ValueMapper[SV, EVR] = (v) => mapper(v)
    inner.mapValues[VR](mapperJ, materialized)
  }

  def toStream: KStreamS[K, V] =
    inner.toStream

  def toStream[KR](mapper: (K, V) => KR): KStreamS[KR, V] = {
    val mapperJ: KeyValueMapper[K, V, KR] = new KeyValueMapper[K, V, KR]{
      override def apply(k: K, v: V): KR = mapper(k, v)
    }
    inner.toStream[KR](mapperJ)
  }

  def groupBy[KR, VR, SK >: K, SV >: V](selector: (SK, SV) => (KR, VR)): KGroupedTableS[KR, VR] = {
    val selectorJ: KeyValueMapper[SK, SV, KeyValue[KR, VR]] = new KeyValueMapper[SK, SV, KeyValue[KR, VR]]{
      override def apply(key: SK, value: SV): KeyValue[KR, VR] = {
        val res = selector(key, value)
        new KeyValue[KR, VR](res._1, res._2)
      }
    }
    inner.groupBy(selectorJ)
  }

  def groupBy[KR, VR, SK >: K, SV >: V](selector: (SK, SV) => (KR, VR),
    serialized: Serialized[KR, VR]): KGroupedTableS[KR, VR] = {

    val selectorJ: KeyValueMapper[SK, SV, KeyValue[KR, VR]] = new KeyValueMapper[SK, SV, KeyValue[KR, VR]]{
      override def apply(key: SK, value: SV): KeyValue[KR, VR] = {
        val res = selector(key, value)
        new KeyValue[KR, VR](res._1, res._2)
      }
    }
    inner.groupBy(selectorJ, serialized)
  }

  def join[VO, VR, SV >: V, SVO >: VO, EVR <: VR](other: KTableS[K, VO],
    joiner: (SV, SVO) => EVR): KTableS[K, VR] = {

    val joinerJ: ValueJoiner[SV, SVO, EVR] = new ValueJoiner[SV, SVO, EVR]{
      override def apply(value1: SV, value2: SVO): EVR = joiner(value1, value2)
    }
    inner.join[VO, VR](other.inner, joinerJ)
  }

  def join[VO, VR, SV >: V, SVO >: VO, EVR <: VR](other: KTableS[K, VO],
    joiner: (SV, SVO) => EVR,
    materialized: Materialized[K, VR, KeyValueStore[Bytes, Array[Byte]]]): KTableS[K, VR] = {

    val joinerJ: ValueJoiner[SV, SVO, EVR] = new ValueJoiner[SV, SVO, EVR]{
      override def apply(value1: SV, value2: SVO): EVR = joiner(value1, value2)
    }
    inner.join[VO, VR](other.inner, joinerJ, materialized)
  }

  def leftJoin[VO, VR, SV >: V, SVO >: VO, EVR <: VR](other: KTableS[K, VO],
    joiner: (SV, SVO) => EVR): KTableS[K, VR] = {

    val joinerJ: ValueJoiner[SV, SVO, EVR] = new ValueJoiner[SV, SVO, EVR]{
      override def apply(value1: SV, value2: SVO): EVR = joiner(value1, value2)
    }
    inner.leftJoin[VO, VR](other.inner, joinerJ)
  }

  def leftJoin[VO, VR, SV >: V, SVO >: VO, EVR <: VR](other: KTableS[K, VO],
    joiner: (SV, SVO) => EVR,
    materialized: Materialized[K, VR, KeyValueStore[Bytes, Array[Byte]]]): KTableS[K, VR] = {

    val joinerJ: ValueJoiner[SV, SVO, EVR] = new ValueJoiner[SV, SVO, EVR]{
      override def apply(value1: SV, value2: SVO): EVR = joiner(value1, value2)
    }
    inner.leftJoin[VO, VR](other.inner, joinerJ, materialized)
  }

  def outerJoin[VO, VR, SV >: V, SVO >: VO, EVR <: VR](other: KTableS[K, VO],
    joiner: (SV, SVO) => EVR): KTableS[K, VR] = {

    val joinerJ: ValueJoiner[SV, SVO, EVR] = new ValueJoiner[SV, SVO, EVR]{
      override def apply(value1: SV, value2: SVO): EVR = joiner(value1, value2)
    }
    inner.outerJoin[VO, VR](other.inner, joinerJ)
  }

  def outerJoin[VO, VR, SV >: V, SVO >: VO, EVR <: VR](other: KTableS[K, VO],
    joiner: (SV, SVO) => EVR,
    materialized: Materialized[K, VR, KeyValueStore[Bytes, Array[Byte]]]): KTableS[K, VR] = {

    val joinerJ: ValueJoiner[SV, SVO, EVR] = new ValueJoiner[SV, SVO, EVR]{
      override def apply(value1: SV, value2: SVO): EVR = joiner(value1, value2)
    }
    inner.outerJoin[VO, VR](other.inner, joinerJ, materialized)
  }

  def queryableStoreName: String =
    inner.queryableStoreName
}
