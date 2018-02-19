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

import com.lightbend.kafka.scala.streams.typesafe.unsafe.ConverterToTypeSafer
import org.apache.kafka.streams.kstream._

/** Conversions to keep the underlying abstraction from leaking. These allow
  * us to always return a TS object instead of the underlying one.
  *
  * These conversions are all Value Classes (extends AnyVal), which means
  * that no new objects get allocated for the `.safe` call (only the TSxx
  * objects get allocated).
  */
object ImplicitConverters {

  implicit final class TSKStreamAuto[K, V]
  (val inner: KStream[K, V])
    extends
      AnyVal {
    def safe: TSKStream[K, V] =
      new TSKStream[K, V](inner)
  }

  implicit final class TSKTableAuto[K, V]
  (val inner: KTable[K, V])
    extends AnyVal {
    def safe: TSKTable[K, V] =
      new TSKTable[K, V](inner)
  }

  implicit final class TSKGroupedStreamAuto[K, V]
  (val inner: KGroupedStream[K, V])
    extends AnyVal {
    def safe: TSKGroupedStream[K, V] =
      new TSKGroupedStream[K, V](inner)
  }

  implicit final class TSKGroupedTableAuto[K, V]
  (val inner: KGroupedTable[K, V])
    extends AnyVal {
    def safe: TSKGroupedTable[K, V] =
      new TSKGroupedTable[K, V](inner)
  }

  implicit final class TSSessionWindowedKStreamAuto[K, V]
  (val inner: SessionWindowedKStream[K, V])
    extends AnyVal {
    def safe: TSSessionWindowedKStream[K, V] =
      new TSSessionWindowedKStream[K, V](inner)
  }

  implicit final class TSTimeWindowedKStreamAuto[K, V]
  (val inner: TimeWindowedKStream[K, V])
    extends AnyVal {
    def safe: TSTimeWindowedKStream[K, V] =
      new TSTimeWindowedKStream[K, V](inner)
  }

  implicit object TSKGroupedStreamAuto
    extends ConverterToTypeSafer[KGroupedStream, TSKGroupedStream] {
    override def safe[K, V](src: KGroupedStream[K, V]): TSKGroupedStream[K, V] =
      src.safe
  }

  implicit object TSKStreamAuto
    extends ConverterToTypeSafer[KStream, TSKStream] {
    override def safe[K, V](src: KStream[K, V]): TSKStream[K, V] = src.safe
  }

  implicit object TSKTableAuto
    extends ConverterToTypeSafer[KTable, TSKTable] {
    override def safe[K, V](src: KTable[K, V]): TSKTable[K, V] = src.safe
  }

  implicit object TSKGroupedTableAuto
    extends ConverterToTypeSafer[KGroupedTable, TSKGroupedTable] {
    override def safe[K, V](src: KGroupedTable[K, V])
    : TSKGroupedTable[K, V] = src.safe
  }

  implicit object TSSessionWindowedKStreamAuto
    extends ConverterToTypeSafer[SessionWindowedKStream,
      TSSessionWindowedKStream] {
    override def safe[K, V](src: SessionWindowedKStream[K, V])
    : TSSessionWindowedKStream[K, V] = src.safe
  }

  implicit object TSTimeWindowedKStreamAuto
    extends ConverterToTypeSafer[TimeWindowedKStream, TSTimeWindowedKStream] {
    override def safe[K, V](src: TimeWindowedKStream[K, V])
    : TSTimeWindowedKStream[K, V] = src.safe
  }

}
