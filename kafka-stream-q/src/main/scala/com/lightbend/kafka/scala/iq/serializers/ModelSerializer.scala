package com.lightbend.kafka.scala.iq
package serializers

import org.apache.kafka.common.serialization.{Serializer, Deserializer}

import java.util.Map

import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._


class ModelSerializer[T : Encoder : Decoder] extends Serializer[T] with Deserializer[T] {

  override def configure(configs: Map[String, _], isKey: Boolean) = {}

  override def serialize(topic: String, t: T): Array[Byte] =
    t.asJson.noSpaces.getBytes(CHARSET)

  override def deserialize(topic: String, bytes: Array[Byte]): T =
    decode[T](new String(bytes, CHARSET)) match {
      case Right(t) => t
      case Left(err) => throw new IllegalArgumentException(err.toString)
    }

  override def close(): Unit = {}
}
