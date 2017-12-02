package com.lightbend.kafka.scala.iq.example
package serializers

import org.apache.kafka.common.serialization.Deserializer

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

import java.util.{ Map => JMap }

import io.confluent.kafka.serializers.KafkaAvroDeserializer

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG

class SpecificAvroDeserializerWithSchemaRegistry[T <: org.apache.avro.specific.SpecificRecord] extends Deserializer[T] {

  val inner: KafkaAvroDeserializer = new KafkaAvroDeserializer()

  override def configure(configs: JMap[String, _], isKey: Boolean): Unit = {
    val effectiveConfigs = Map(SPECIFIC_AVRO_READER_CONFIG -> true) ++ configs.asScala
    inner.configure(effectiveConfigs.asJava, isKey)
  }

  override def deserialize(s: String, bytes: Array[Byte]): T = inner.deserialize(s, bytes).asInstanceOf[T]

  override def close(): Unit = inner.close()
}
