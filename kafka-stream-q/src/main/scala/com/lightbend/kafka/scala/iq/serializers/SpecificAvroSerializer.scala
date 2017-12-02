package com.lightbend.kafka.scala.iq
package serializers

import org.apache.kafka.common.serialization.Serializer

import com.twitter.bijection.Injection
import org.apache.avro.specific.SpecificRecordBase

import java.util.{ Map => JMap }

class SpecificAvroSerializer[T <: org.apache.avro.specific.SpecificRecordBase](injection: Injection[T, Array[Byte]]) extends Serializer[T] {

  override def configure(configs: JMap[String, _], isKey: Boolean): Unit = ()

  override def serialize(topic: String, record: T): Array[Byte] = {
    injection.apply(record)
  }

  override def close(): Unit = ()
}
