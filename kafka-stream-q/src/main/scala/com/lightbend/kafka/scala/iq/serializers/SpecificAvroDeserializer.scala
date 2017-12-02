package com.lightbend.kafka.scala.iq
package serializers

import org.apache.kafka.common.serialization.Deserializer

import com.twitter.bijection.Injection
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.Schema

import scala.util.{Success, Failure}

import java.util.{ Map => JMap }

class SpecificAvroDeserializer[T <: org.apache.avro.specific.SpecificRecordBase](injection: Injection[T, Array[Byte]]) extends Deserializer[T] {

  override def configure(configs: JMap[String, _], isKey: Boolean): Unit = ()

  override def deserialize(s: String, bytes: Array[Byte]): T = {
    injection.invert(bytes) match {
      case Success(t) => t
      case Failure(ex) => throw new Exception(ex)
    }
  }

  override def close(): Unit = ()
}
