package com.lightbend.kafka.scala.streams

import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.KeyValue

import scala.language.implicitConversions

object ImplicitConversions {

  implicit def wrapKStream[K, V](inner: KStream[K, V]): KStreamS[K, V] =
    new KStreamS[K, V](inner)

  implicit def wrapKGroupedStream[K, V](inner: KGroupedStream[K, V]): KGroupedStreamS[K, V] =
    new KGroupedStreamS[K, V](inner)

  implicit def wrapSessionWindowedKStream[K, V](inner: SessionWindowedKStream[K, V]): SessionWindowedKStreamS[K, V] =
    new SessionWindowedKStreamS[K, V](inner)

  implicit def wrapTimeWindowedKStream[K, V](inner: TimeWindowedKStream[K, V]): TimeWindowedKStreamS[K, V] =
    new TimeWindowedKStreamS[K, V](inner)

  implicit def wrapKTable[K, V](inner: KTable[K, V]): KTableS[K, V] =
    new KTableS[K, V](inner)

  implicit def wrapKGroupedTable[K, V](inner: KGroupedTable[K, V]): KGroupedTableS[K, V] =
    new KGroupedTableS[K, V](inner)

  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)

  implicit class PredicateFromFunction[K, V](val test: (K, V) => Boolean) extends AnyVal {
    def asPredicate: Predicate[K,V] = test(_,_)
  }

  implicit class MapperFromFunction[K, V, R](val f:(K,V) => R) extends AnyVal {
    def asKeyValueMapper: KeyValueMapper[K, V, R] = (k: K, v: V) => f(k, v)
    def asValueJoiner: ValueJoiner[K,V,R] = (v1, v2) => f(v1, v2)
  }

  implicit class KeyValueMapperFromFunction[K, V, KR, VR](val f:(K,V) => (KR, VR)) extends AnyVal {
    def asKeyValueMapper: KeyValueMapper[K, V, KeyValue[KR, VR]] = (k, v) => {
        val (kr, vr) = f(k, v)
        KeyValue.pair(kr, vr)
    }
  }

  implicit class ValueMapperFromFunction[V, VR](val f: V => VR) extends AnyVal {
    def asValueMapper: ValueMapper[V, VR] = v => f(v)
  }

  implicit class AggregatorFromFunction[K, V, VR](val f: (K, V, VR) => VR) extends AnyVal {
    def asAggregator: Aggregator[K, V, VR] = (k,v,r) => f(k,v,r)
  }


}

