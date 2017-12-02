package com.lightbend.kafka.scala.iq.example
package serializers

import org.apache.kafka.common.serialization.{ Deserializer, Serde, Serdes, Serializer }

import java.util.Collections
import java.util.Map

class SpecificAvroSerdeWithSchemaRegistry[T <: org.apache.avro.specific.SpecificRecord] extends Serde[T] {

  val inner: Serde[T] = Serdes.serdeFrom(new SpecificAvroSerializerWithSchemaRegistry[T](), new SpecificAvroDeserializerWithSchemaRegistry[T]()) 

  override def serializer(): Serializer[T] = inner.serializer()

  override def deserializer(): Deserializer[T] = inner.deserializer()

  override def configure(configs: Map[String, _], isKey: Boolean): Unit = {
    inner.serializer().configure(configs, isKey)
    inner.deserializer().configure(configs, isKey)
  }

  override def close(): Unit = {
    inner.serializer().close()
    inner.deserializer().close()
  }
}
