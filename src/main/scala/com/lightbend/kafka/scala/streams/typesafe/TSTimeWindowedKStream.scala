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
import com.lightbend.kafka.scala.streams.typesafe.ImplicitConverters._
import org.apache.kafka.streams.kstream.{Materialized, TimeWindowedKStream,
  Windowed}

class TSTimeWindowedKStream[K, V]
(protected[typesafe] override val unsafe: TimeWindowedKStream[K, V])
  extends AnyVal with TSKType[TimeWindowedKStream, K, V] {

  def aggregate[VR](initializer: => VR,
                    aggregator: (K, V, VR) => VR)
                   (implicit materialized: Materialized[K, VR, wsb])
  : TSKTable[Windowed[K], VR] = {
    unsafe.aggregate(initializer.asInitializer,
      aggregator.asAggregator,
      materialized)
      .safe
  }

  def count(implicit materialized: Materialized[K, java.lang.Long, wsb])
  : TSKTable[Windowed[K], Long] = {
    unsafe
      .count(materialized)
      .mapValues[scala.Long]({
      l: java.lang.Long => Long2long(l)
    }.asValueMapper).safe
  }

  def reduce(reducer: (V, V) => V)
            (implicit materialized: Materialized[K, V, wsb])
  : TSKTable[Windowed[K], V] = {
    unsafe
      .reduce(reducer.asReducer, materialized)
      .safe
  }
}
