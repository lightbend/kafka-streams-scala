package com.lightbend.kafka.scala.iq.example
package serializers

import org.apache.kafka.common.serialization.{ Deserializer, Serializer }

import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

class Tuple2Serializer[T : Encoder : Decoder,
                       U : Encoder : Decoder] extends Serializer[(T, U)] with Deserializer[(T, U)] {

  override def configure(configs: java.util.Map[String, _], isKey: Boolean) = {}

  override def serialize(topic: String, data: (T, U)) =
    data.asJson.noSpaces.getBytes(CHARSET)

  override def deserialize(topic: String, bytes: Array[Byte]) = {
    decode[(T, U)](new String(bytes, CHARSET)) match {
      case Right(t) => t
      case Left(err) => throw new IllegalArgumentException(err.toString)
    }
  }

  override def close() = {}
}

