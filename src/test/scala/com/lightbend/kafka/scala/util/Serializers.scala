package com.lightbend.kafka.scala.util

import org.apache.kafka.common.serialization.Serdes

trait Serializers {
  final val sls = new ScalaLongSerializer
  final val scalaLongSerde = Serdes.serdeFrom(sls, sls)
}
