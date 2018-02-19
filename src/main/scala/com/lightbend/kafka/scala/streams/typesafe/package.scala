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
package com.lightbend.kafka.scala.streams

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.state.{KeyValueStore, SessionStore, WindowStore}

/** Provides a more type-safe interface for Kafka Streams when using Scala.
  *
  * In particular, it always requires a Materialized instance to be provided
  * to aggregating functions that may persist data, so that you won't ever
  * find yourself using the default Serde and thus converting runtime
  * exceptions in compile-time errors.
  *
  * You don't need to create all Materialized (and other persisting helper)
  * objects, as they can be deduced by the Scala type system when you have
  * the implicit resolution rules available at
  * [[com.lightbend.kafka.scala.streams.typesafe.SerdeDerivations]] in scope.
  *
  */
package object typesafe {
  private[typesafe] type kvs = KeyValueStore[Bytes, Array[Byte]]
  private[typesafe] type ssb = SessionStore[Bytes, Array[Byte]]
  private[typesafe] type wsb = WindowStore[Bytes, Array[Byte]]
}
