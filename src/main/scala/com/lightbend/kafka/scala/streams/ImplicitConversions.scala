/**
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package com.lightbend.kafka.scala.streams

import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.{ KeyValue, Consumed }
import org.apache.kafka.common.serialization.Serde

import scala.language.implicitConversions

/**
 * Implicit conversions between the Scala wrapper objects and the underlying Java
 * objects.
 */
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

  implicit def tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)

  // technique for optional implicits adopted from
  // http://missingfaktor.blogspot.in/2013/12/optional-implicit-trick-in-scala.html

  case class Perhaps[E](value: Option[E]) {
    def fold[F](ifAbsent: => F)(ifPresent: E => F): F = {
      value.fold(ifAbsent)(ifPresent)
    }
  }

  //scalastyle:off null
  implicit def perhaps[E](implicit ev: E = null): Perhaps[E] = Perhaps(Option(ev))
  //scalastyle:on null
  // we would also like to allow users implicit serdes
  // and these implicits will convert them to `Serialized`, `Produced` or `Consumed`

  implicit def serializedFromSerde[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]): Serialized[K, V] =
    Serialized.`with`(keySerde, valueSerde)

  implicit def consumedFromSerde[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]): Consumed[K, V] =
    Consumed.`with`(keySerde, valueSerde)

  implicit def producedFromSerde[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]): Produced[K, V] =
    Produced.`with`(keySerde, valueSerde)

  implicit def joinedFromKVOSerde[K, V, VO](implicit keySerde: Serde[K], valueSerde: Serde[V],
                                            otherValueSerde: Serde[VO]): Joined[K, V, VO] =
    Joined.`with`(keySerde, valueSerde, otherValueSerde)
}
