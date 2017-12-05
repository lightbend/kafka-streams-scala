package com.lightbend.kafka.scala.iq.example
package serializers

import models.LogRecord
import org.apache.kafka.common.serialization.Serdes
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import com.lightbend.kafka.scala.iq.serializers._

trait AppSerializers extends Serializers {
  final val ts = new Tuple2Serializer[String, String]()
  final val ms = new ModelSerializer[LogRecord]()
  final val logRecordSerde = Serdes.serdeFrom(ms, ms)
  final val tuple2StringSerde = Serdes.serdeFrom(ts, ts)
}
