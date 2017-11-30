package com.lightbend.kafka.scala.iq
package serializers

import org.apache.kafka.common.serialization.{ Deserializer, Serde, Serdes, Serializer }

import org.apache.avro.Schema

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs

import java.util.Collections
import java.util.Map

class SpecificAvroSerde[T <: org.apache.avro.specific.SpecificRecordBase](schema: Schema) extends Serde[T] {

  val recordInjection: Injection[T, Array[Byte]] = SpecificAvroCodecs.toBinary(schema)

  override def serializer(): Serializer[T] = new SpecificAvroSerializer(recordInjection)

  override def deserializer(): Deserializer[T] = new SpecificAvroDeserializer(recordInjection)

  override def configure(configs: Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()
}
