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


/** These are implicit serdes that can be mixed-in into your Kafka Streams
  * job, making it easier to work from scala.
  *
  * Objects such as [[util.Materialized]],
  * [[org.apache.kafka.streams.Consumed]],
  * [[org.apache.kafka.streams.kstream.Produced]] or
  * [[org.apache.kafka.streams.kstream.Joined]] need to be created from
  * [[org.apache.kafka.common.serialization.Serde]] instances according to the
  * types you are handling. When including this class, you will automatically
  * generate these objects from available implicit Serde instances.
  */
trait SerdeDerivations extends derivations.Default

object SerdeDerivations extends SerdeDerivations
