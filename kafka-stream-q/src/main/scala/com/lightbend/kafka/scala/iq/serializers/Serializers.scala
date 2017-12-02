package com.lightbend.kafka.scala.iq
package serializers

import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.common.serialization.{ Serde, Serdes, StringSerializer, StringDeserializer, ByteArraySerializer, ByteArrayDeserializer }
import org.apache.kafka.streams.kstream.internals.{ WindowedSerializer, WindowedDeserializer }

trait Serializers {
  final val stringSerializer = new StringSerializer()
  final val stringDeserializer = new StringDeserializer()
  final val byteArraySerializer = new ByteArraySerializer()
  final val byteArrayDeserializer = new ByteArrayDeserializer()

  final val windowedStringSerializer: WindowedSerializer[String] = new WindowedSerializer[String](stringSerializer)
  final val windowedStringDeserializer: WindowedDeserializer[String] = new WindowedDeserializer[String](stringDeserializer)
  final val windowedStringSerde: Serde[Windowed[String]] = Serdes.serdeFrom(windowedStringSerializer, windowedStringDeserializer)

  final val windowedByteArraySerializer: WindowedSerializer[Array[Byte]] = new WindowedSerializer[Array[Byte]](byteArraySerializer)
  final val windowedByteArrayDeserializer: WindowedDeserializer[Array[Byte]] = new WindowedDeserializer[Array[Byte]](byteArrayDeserializer)
  final val windowedByteArraySerde: Serde[Windowed[Array[Byte]]] = Serdes.serdeFrom(windowedByteArraySerializer, windowedByteArrayDeserializer)

  final val stringSerde = Serdes.String()
  final val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]
  final val byteArraySerde = Serdes.ByteArray()
}
