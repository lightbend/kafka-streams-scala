package com.lightbend.kafka.scala.streams.typesafe

import org.apache.kafka.streams.kstream._

/** Conversions to keep the underlying abstraction from leaking. These allow
  * us to always return a TS object instead of the underlying one.
  *
  * These conversions are all Value Classes (extends AnyVal), which means
  * that no new objects get allocated for the `.safe` call (only the TSxx
  * objects get allocated).
  *
  * @author Santiago Saavedra (ssaavedra@openshine.com)
  */
private[typesafe] object implicits {

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

}
