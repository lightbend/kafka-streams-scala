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
import org.apache.kafka.streams.kstream.{Materialized,
  SessionWindowedKStream, Windowed}

class TSSessionWindowedKStream[K, V]
(protected[typesafe] override val unsafe: SessionWindowedKStream[K, V])
  extends AnyVal with TSKType[SessionWindowedKStream, K, V] {

  def aggregate[VR](initializer: => VR,
                    aggregator: (K, V, VR) => VR,
                    merger: (K, VR, VR) => VR)
                   (implicit materialized: Materialized[K, VR, ssb])
  : TSKTable[Windowed[K], VR] = {
    unsafe.aggregate(
      initializer.asInitializer,
      aggregator.asAggregator,
      merger.asMerger,
      materialized)
      .safe
  }

  def count(implicit materialized: Materialized[K, java.lang.Long, ssb])
  : TSKTable[Windowed[K], Long] =
    unsafe
      .count(materialized)
      .mapValues[scala.Long]({
      l: java.lang.Long => Long2long(l)
    }.asValueMapper).safe


  def reduce(reducer: (V, V) => V)
            (implicit materialized: Materialized[K, V, ssb])
  : TSKTable[Windowed[K], V] = {
    unsafe.reduce(reducer.asReducer, materialized).safe
  }

}
