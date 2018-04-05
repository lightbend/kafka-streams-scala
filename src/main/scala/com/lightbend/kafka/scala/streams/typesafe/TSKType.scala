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

import scala.language.higherKinds

/** A base type for all base stream classes in the typesafe API.
  */
trait TSKType[T[_, _], K, V] extends Any {
  protected[typesafe] def unsafe: T[K, V]
}
