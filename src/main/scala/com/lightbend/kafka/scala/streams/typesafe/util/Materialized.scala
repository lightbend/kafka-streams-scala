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

package com.lightbend.kafka.scala.streams.typesafe.util

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{Materialized => Base}
import org.apache.kafka.streams.processor.StateStore

import scala.language.implicitConversions

/** Convenience object for creating Materialized objects from Scala.
  *
  * You can create a materialized from (implicit or explicit) Serde instances
  * by calling .apply as such:
  * {{{
  *   Materialized[String, MyType]
  * }}}
  *
  * You can also name your Materialized by using [[Materialized.as()]], with
  * these two syntaxes:
  *
  * {{{
  *   Materialized[String, MyType].as[WindowStore]("Hello world")
  *
  *   Materialized.as[String, MyType, WindowStore]("Hello world")
  * }}}
  *
  * The third type parameter may not be required in cases where it can be
  * inferred by the Scala type system, but is provided in the example for
  * completeness.
  */
object Materialized {
  def apply[K, V](implicit keySerde: Serde[K],
                  valueSerde: Serde[V]): MaterializedBuilder[K, V] = {
    new MaterializedBuilder(keySerde, valueSerde)
  }

  def as[K, V, S <: StateStore](stateStoreName: String)
                               (implicit keySerde: Serde[K],
                                valueSerde: Serde[V]): Base[K, V, S] = {
    Base.as(stateStoreName).withKeySerde(keySerde).withValueSerde(valueSerde)
  }

  class MaterializedBuilder[K, V](val keySerde: Serde[K],
                                  val valueSerde: Serde[V]) {
    def as[S <: StateStore](stateStoreName: String): Base[K, V, S] =
      Base.as(stateStoreName).withKeySerde(keySerde).withValueSerde(valueSerde)

  }

  implicit def builderAsBase[K, V, S <: StateStore]
  (builder: MaterializedBuilder[K, V]): Base[K, V, S] =
    Base.`with`(builder.keySerde, builder.valueSerde)

}
