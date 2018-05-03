/**
  * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
  * Copyright 2017-2018 Alexis Seigneurin.
  */
package com.lightbend.kafka.scala.streams

import java.util
import org.apache.kafka.common.serialization.{
  Serde,
  Serdes => JavaSerdes,
  Deserializer => JavaDeserializer,
  Serializer => JavaSerializer
}

trait Serdes {
  implicit val string: Serde[String]                             = JavaSerdes.String()
  implicit val long: Serde[Long]                                 = JavaSerdes.Long().asInstanceOf[Serde[Long]]
  implicit val javaLong: Serde[java.lang.Long]                   = JavaSerdes.Long()
  implicit val byteArray: Serde[Array[Byte]]                     = JavaSerdes.ByteArray()
  implicit val bytes: Serde[org.apache.kafka.common.utils.Bytes] = JavaSerdes.Bytes()
  implicit val float: Serde[Float]                               = JavaSerdes.Float().asInstanceOf[Serde[Float]]
  implicit val javaFloat: Serde[java.lang.Float]                 = JavaSerdes.Float()
  implicit val double: Serde[Double]                             = JavaSerdes.Double().asInstanceOf[Serde[Double]]
  implicit val javaDouble: Serde[java.lang.Double]               = JavaSerdes.Double()
  implicit val integer: Serde[Int]                               = JavaSerdes.Integer().asInstanceOf[Serde[Int]]
  implicit val javaInteger: Serde[java.lang.Integer]             = JavaSerdes.Integer()

  def fromFn[T >: Null](serializer: T => Array[Byte], deserializer: Array[Byte] => Option[T]): Serde[T] =
    JavaSerdes.serdeFrom(
      new JavaSerializer[T] {
        override def serialize(topic: String, data: T): Array[Byte]                = serializer(data)
        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
        override def close(): Unit                                                 = ()
      },
      new JavaDeserializer[T] {
        override def deserialize(topic: String, data: Array[Byte]): T              = deserializer(data).orNull
        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
        override def close(): Unit                                                 = ()
      }
    )

  def fromFn[T >: Null](serializer: (String, T) => Array[Byte],
                        deserializer: (String, Array[Byte]) => Option[T]): Serde[T] =
    JavaSerdes.serdeFrom(
      new JavaSerializer[T] {
        override def serialize(topic: String, data: T): Array[Byte]                = serializer(topic, data)
        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
        override def close(): Unit                                                 = ()
      },
      new JavaDeserializer[T] {
        override def deserialize(topic: String, data: Array[Byte]): T              = deserializer(topic, data).orNull
        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
        override def close(): Unit                                                 = ()
      }
    )
}

/**
  * Implicit values for default serdes
  */
object Serdes extends Serdes

@deprecated("Use com.lightbend.kafka.scala.streams.Serdes instead", "")
object DefaultSerdes extends Serdes

@deprecated("Use com.lightbend.kafka.scala.streams.Serdes.fromFn instead", "")
trait StatelessSerde[T >: Null] extends Serde[T] {
  def serialize(data: T): Array[Byte]
  def deserialize(data: Array[Byte]): Option[T]

  override def deserializer(): Deserializer[T] =
    (data: Array[Byte]) => deserialize(data)

  override def serializer(): Serializer[T] =
    (data: T) => serialize(data)

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()
}

@deprecated("Use org.apache.kafka.common.serialization.Deserializer instead", "")
trait Deserializer[T >: Null] extends JavaDeserializer[T] {
  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()

  override def deserialize(topic: String, data: Array[Byte]): T =
    Option(data).flatMap(deserialize).orNull

  def deserialize(data: Array[Byte]): Option[T]
}

@deprecated("Use org.apache.kafka.common.serialization.Serializer instead", "")
trait Serializer[T] extends JavaSerializer[T] {
  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()

  override def serialize(topic: String, data: T): Array[Byte] =
    Option(data).map(serialize).orNull

  def serialize(data: T): Array[Byte]
}
