package com.lightbend.kafka.scala.streams

import minitest._
import com.lightbend.kafka.scala.server.KafkaLocalServer


object KafkaStreamsTest extends TestSuite[KafkaLocalServer] {
  def setup(): KafkaLocalServer = {
    KafkaLocalServer(true)
  }

  def tearDown(env: KafkaLocalServer): Unit = {
    env.stop()
  }

  test("simple test") { env =>
    assert(env != null)
  }
}

