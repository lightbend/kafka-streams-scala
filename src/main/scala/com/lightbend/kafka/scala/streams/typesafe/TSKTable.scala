/*
 * Copyright 2018 OpenShine SL <https://www.openshine.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lightbend.kafka.scala.streams.typesafe

import com.lightbend.kafka.scala.streams.FunctionConversions._
import com.lightbend.kafka.scala.streams.typesafe.implicits._
import org.apache.kafka.streams.{Consumed, StreamsBuilder}
import org.apache.kafka.streams.kstream.{KTable, Materialized, Serialized}

import scala.language.higherKinds

class TSKTable[K, V](protected[typesafe] override val unsafe: KTable[K, V])
  extends AnyVal with TSKType[KTable, K, V] {

  def map[KR, VR](selector: (K, V) => (KR, VR))
                 (implicit serialized: Serialized[KR, VR])
  : TSKGroupedTable[KR, VR] =
    unsafe
      .groupBy(selector.asKeyValueMapper, serialized)
      .safe

  def mapValues[VR](mapper: V => VR)
                   (implicit materialized: Materialized[K, VR, kvs])
  : TSKTable[K, VR] =
    unsafe
      .mapValues[VR](mapper.asValueMapper, materialized)
      .safe

  def filterValues(predicate: V => Boolean)
                  (implicit materialized: Materialized[K, V, kvs])
  : TSKTable[K, V] = this.filter((k, v) => predicate(v))

  def filter(predicate: (K, V) => Boolean)
            (implicit materialized: Materialized[K, V, kvs])
  : TSKTable[K, V] =
    unsafe
      .filter(predicate.asPredicate, materialized)
      .safe

  def filterNot(predicate: (K, V) => Boolean)
               (implicit materialized: Materialized[K, V, kvs])
  : TSKTable[K, V] =
    unsafe
      .filterNot(predicate.asPredicate, materialized)
      .safe

  def toStream: TSKStream[K, V] = unsafe.toStream.safe

  def toStream[KR](keyMapper: (K, V) => KR): TSKStream[KR, V] =
    unsafe
      .toStream[KR](keyMapper.asKeyValueMapper)
      .safe

  def groupBy[KR, VR](selector: (K, V) => (KR, VR))
                     (implicit serialized: Serialized[KR, VR])
  : TSKGroupedTable[KR, VR]
  = unsafe
    .groupBy(selector.asKeyValueMapper, serialized)
    .safe

  def join[VO, VR](other: TSKTable[K, VO],
                   joiner: (V, VO) => VR)
                  (implicit materialized: Materialized[K, VR, kvs])
  : TSKTable[K, VR] =
    unsafe
      .join[VO, VR](other.unsafe, joiner.asValueJoiner, materialized)
      .safe

  def leftJoin[VO, VR](other: TSKTable[K, VO],
                       joiner: (V, VO) => VR)
                      (implicit materialized: Materialized[K, VR, kvs])
  : TSKTable[K, VR] =
    unsafe
      .leftJoin[VO, VR](other.unsafe, joiner.asValueJoiner, materialized)
      .safe

  def outerJoin[VO, VR](other: TSKTable[K, VO],
                        joiner: (V, VO) => VR)
                       (implicit materialized: Materialized[K, VR, kvs])
  : TSKTable[K, VR] =
    unsafe
      .outerJoin[VO, VR](other.unsafe, joiner.asValueJoiner, materialized)
      .safe

  def queryableStoreName: String = unsafe.queryableStoreName()

}

object TSKTable {
  /** Creates a new TSKStream from a topic, given an implicit StreamsBuilder
    * and the appropriate Serde instances for the types you want to read from
    * the topic.
    *
    * @param topic the topic name you want to read from
    * @param builder the StreamsBuilder you want to use
    * @param consumed the Consumed instance that contains the deserialization
    *                 procedure from the Kafka topic
    * @tparam K the type of keys to be read from the topic
    * @tparam V the type of values to be read from the topic
    * @return
    */
  def apply[K, V](topic: String)
                 (implicit builder: StreamsBuilder,
                  consumed: Consumed[K, V]): TSKTable[K, V] =
    new TSKTable[K, V](builder.table(topic, consumed))
}
