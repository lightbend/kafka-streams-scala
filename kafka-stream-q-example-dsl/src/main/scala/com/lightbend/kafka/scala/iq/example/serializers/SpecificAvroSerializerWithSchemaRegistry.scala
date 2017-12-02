package com.lightbend.kafka.scala.iq.example
package serializers

import org.apache.kafka.common.serialization.Serializer

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

import java.util.{ Map => JMap }

import io.confluent.kafka.serializers.KafkaAvroSerializer

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG

class SpecificAvroSerializerWithSchemaRegistry[T <: org.apache.avro.specific.SpecificRecord] extends Serializer[T] {

  val inner: KafkaAvroSerializer = new KafkaAvroSerializer()

  override def configure(configs: JMap[String, _], isKey: Boolean): Unit = {
    val effectiveConfigs = Map(SPECIFIC_AVRO_READER_CONFIG -> true) ++ configs.asScala
    inner.configure(effectiveConfigs.asJava, isKey)
  }

  override def serialize(topic: String, record: T): Array[Byte] =
    inner.serialize(topic, record)

  override def close(): Unit = inner.close()
}
