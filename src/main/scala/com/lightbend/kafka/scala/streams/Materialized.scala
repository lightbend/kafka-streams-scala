package com.lightbend.kafka.scala.streams

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{Materialized => JavaMaterialized}
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.state._

object Materialized {

  def as[K, V, S <: StateStore](storeName: String)(implicit keySerde: Serde[K],
                                                   valueSerde: Serde[V]): JavaMaterialized[K, V, S] =
    JavaMaterialized.as(storeName).withKeySerde(keySerde).withValueSerde(valueSerde)

  def as[K, V](
    supplier: WindowBytesStoreSupplier
  )(implicit keySerde: Serde[K], valueSerde: Serde[V]): JavaMaterialized[K, V, WindowStore[Bytes, Array[Byte]]] =
    JavaMaterialized.as(supplier).withKeySerde(keySerde).withValueSerde(valueSerde)

  def as[K, V](
    supplier: SessionBytesStoreSupplier
  )(implicit keySerde: Serde[K], valueSerde: Serde[V]): JavaMaterialized[K, V, SessionStore[Bytes, Array[Byte]]] =
    JavaMaterialized.as(supplier).withKeySerde(keySerde).withValueSerde(valueSerde)

  def as[K, V](
    supplier: KeyValueBytesStoreSupplier
  )(implicit keySerde: Serde[K], valueSerde: Serde[V]): JavaMaterialized[K, V, KeyValueStore[Bytes, Array[Byte]]] =
    JavaMaterialized.as(supplier).withKeySerde(keySerde).withValueSerde(valueSerde)
}
