package com.lightbend.kafka.scala.iq.example
package processor

import java.util

import com.twitter.algebird.BF
import com.twitter.chill.ScalaKryoInstantiator
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization._

class BFSerializer[T] extends Serializer[BF[T]] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    // nothing to do
  }

  override def serialize(topic: String, bf: BF[T]): Array[Byte] =
    if (bf == null) null
    else ScalaKryoInstantiator.defaultPool.toBytesWithClass(bf)

  override def close(): Unit = {
    // nothing to do
  }

}

class BFDeserializer[T] extends Deserializer[BF[T]] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    // nothing to do
  }

  override def deserialize(topic: String, bytes: Array[Byte]): BF[T] =
    if (bytes == null) null
    else if (bytes.isEmpty) throw new SerializationException("byte array must not be empty")
    else ScalaKryoInstantiator.defaultPool.fromBytes(bytes).asInstanceOf[BF[T]]

  override def close(): Unit = {
    // nothing to do
  }

}

object BFSerde {

  def apply[T]: Serde[BF[T]] = Serdes.serdeFrom(new BFSerializer[T], new BFDeserializer[T])

}

