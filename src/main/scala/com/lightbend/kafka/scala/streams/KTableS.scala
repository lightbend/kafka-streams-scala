package com.lightbend.kafka.scala.streams

// Based on https://github.com/aseigneurin/kafka-streams-scala/blob/master/src/main/scala/com/github/aseigneurin/kafka/streams/scala/KTableS.scala

import ImplicitConversions._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.StreamPartitioner

class KTableS[K, V](val inner: KTable[K, V]) {

  def filter(predicate: (K, V) => Boolean): KTableS[K, V] = {
    val predicateJ: Predicate[K, V] = new Predicate[K, V]{
      override def test(k: K, v: V): Boolean = predicate(k, v)
    }
    inner.filter(predicateJ)
  }

  def filterNot(predicate: (K, V) => Boolean): KTableS[K, V] = {
    val predicateJ: Predicate[K, V] = new Predicate[K, V]{
      override def test(key: K, value: V): Boolean = predicate(key, value)
    }
    inner.filterNot(predicateJ)
  }

  def mapValues[VR, A >: V, B <: VR](mapper: (A) => B): KTable[K, VR] = {
    def mapperJ: ValueMapper[V, VR] = (v) => mapper(v)
    inner.mapValues(mapperJ)
  }

  def toStream: KStreamS[K, V] =
    inner.toStream

  def toStream[KR](mapper: (K, V) => KR): KStreamS[KR, V] = {
    val mapperJ: KeyValueMapper[K, V, KR] = new KeyValueMapper[K, V, KR]{
      override def apply(k: K, v: V): KR = mapper(k, v)
    }
    inner.toStream[KR](mapperJ)
  }

  def groupBy[KR, VR](selector: (K, V) => (KR, VR)): KGroupedTableS[KR, VR] = {
    val selectorJ: KeyValueMapper[K, V, KeyValue[KR, VR]] = new KeyValueMapper[K, V, KeyValue[KR, VR]]{
      override def apply(key: K, value: V): KeyValue[KR, VR] = {
        val res = selector(key, value)
        new KeyValue[KR, VR](res._1, res._2)
      }
    }
    inner.groupBy(selectorJ)
  }

  def join[VO, VR](other: KTableS[K, VO],
                   joiner: (V, VO) => VR): KTableS[K, VR] = {
    val joinerJ: ValueJoiner[V, VO, VR] = new ValueJoiner[V, VO, VR]{
      override def apply(value1: V, value2: VO): VR = joiner(value1, value2)
    }
    inner.join[VO, VR](other.inner, joinerJ)
  }

  def leftJoin[VO, VR](other: KTableS[K, VO],
                       joiner: (V, VO) => VR): KTableS[K, VR] = {
    val joinerJ: ValueJoiner[V, VO, VR] = new ValueJoiner[V, VO, VR]{
      override def apply(v1: V, v2: VO): VR = joiner(v1, v2)
    }
    inner.leftJoin[VO, VR](other.inner, joinerJ)
  }

  def outerJoin[VO, VR](other: KTableS[K, VO],
                        joiner: (V, VO) => VR): KTableS[K, VR] = {
    val joinerJ: ValueJoiner[V, VO, VR] = new ValueJoiner[V, VO, VR]{
      override def apply(v1: V, v2: VO): VR = joiner(v1, v2)
    }
    inner.outerJoin[VO, VR](other.inner, joinerJ)
  }

  def getStoreName: String =
    inner.getStoreName

}
