package com.lightbend.kafka.scala.util

import org.apache.kafka.common.serialization.{ Deserializer, Serializer }

import java.nio.charset.Charset

import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

class ScalaLongSerializer extends Serializer[Long] with Deserializer[Long] {

  final val CHARSET = Charset.forName("UTF-8")

  override def configure(configs: java.util.Map[String, _], isKey: Boolean) = {}

  override def serialize(topic: String, data: Long) =
    data.asJson.noSpaces.getBytes(CHARSET)

  override def deserialize(topic: String, bytes: Array[Byte]) = {
    if (bytes == null) 0L
    else {
      decode[Long](new String(bytes, CHARSET)) match {
        case Right(t) => t
        case Left(err) => throw new IllegalArgumentException(err.toString)
      }
    }
  }

  override def close() = {}
}


