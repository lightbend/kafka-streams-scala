/**
* Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
*/

package com.lightbend.kafka.scala.streams

import org.apache.kafka.streams.processor.{AbstractProcessor, Processor, ProcessorSupplier}

/**
  * When adding a processor to Topology, what is added is not a processor itself, but rather a processor supplier
  * This little abstract class hides this level of indirection and provides a default implementation of
  */

abstract class ProcessorS[K,V] extends AbstractProcessor[K,V] with ProcessorSupplier[K,V]{

  override def get(): Processor[K, V] = this
}