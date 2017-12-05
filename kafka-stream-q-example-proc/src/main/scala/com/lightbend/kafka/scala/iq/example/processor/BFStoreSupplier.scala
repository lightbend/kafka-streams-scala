package com.lightbend.kafka.scala.iq.example
package processor

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.state.StoreSupplier
import com.twitter.algebird.Hash128

class BFStoreSupplier[T: Hash128](val name: String,
                                  val serde: Serde[T],
                                  val loggingEnabled: Boolean,
                                  val logConfig: java.util.Map[String, String]) extends StoreSupplier[BFStore[T]] {

  def this(name: String, serde: Serde[T]) {
    this(name, serde, true, new java.util.HashMap[String, String])
  }

  def this(name: String, serde: Serde[T], loggingEnabled: Boolean) {
    this(name, serde, loggingEnabled, new java.util.HashMap[String, String])
  }

  override def get(): BFStore[T] = new BFStore[T](name, width = 1048576)

  override def metricsScope(): String = ""

}

