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
import org.apache.kafka.streams.kstream.{KGroupedTable, Materialized}

/** Wraps the Java class [[KGroupedTable]] and delegates method calls to the
  * underlying Java object. Makes use of implicit parameters for the
  * [[util.Materialized]] instances.
  */
class TSKGroupedTable[K, V]
(protected[typesafe] override val unsafe: KGroupedTable[K, V])
  extends AnyVal with TSKType[KGroupedTable, K, V] {
  def count(implicit materialized: Materialized[K, java.lang.Long, kvs])
  : TSKTable[K, Long] =
    unsafe
      .count(materialized)
      .mapValues[scala.Long](
      { l: java.lang.Long => Long2long(l) }.asValueMapper)
      .safe

  def reduce(adder: (V, V) => V,
             subtractor: (V, V) => V)
            (implicit materialized: Materialized[K, V, kvs]): TSKTable[K, V] =
    unsafe
      .reduce(adder.asReducer, subtractor.asReducer, materialized)
      .safe

  def aggregate[VR](initializer: => VR,
                    adder: (K, V, VR) => VR,
                    subtractor: (K, V, VR) => VR)
                   (implicit materialized: Materialized[K, VR, kvs])
  : TSKTable[K, VR] =
    unsafe
      .aggregate(initializer.asInitializer,
        adder.asAggregator,
        subtractor.asAggregator,
        materialized)
      .safe
}
