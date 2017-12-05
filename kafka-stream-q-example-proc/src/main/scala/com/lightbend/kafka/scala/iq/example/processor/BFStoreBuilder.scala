package com.lightbend.kafka.scala.iq.example
package processor

import org.apache.kafka.streams.state.StoreBuilder
import com.twitter.algebird.Hash128

class BFStoreBuilder[T: Hash128](val storeSupplier: BFStoreSupplier[T]) extends StoreBuilder[BFStore[T]] {

    override def name(): String = storeSupplier.name

    override def build(): BFStore[T] = storeSupplier.get()

    override def logConfig: java.util.Map[String, String] = storeSupplier.logConfig

    override def loggingEnabled(): Boolean = storeSupplier.loggingEnabled

    override def withCachingEnabled(): BFStoreBuilder[T] = this

    override def withLoggingDisabled(): BFStoreBuilder[T] = {
      storeSupplier.logConfig.clear()
      this
    }

    override def withLoggingEnabled(config: java.util.Map[String, String]): BFStoreBuilder[T] = { 
      new BFStoreBuilder[T](
        new BFStoreSupplier(
          storeSupplier.name, 
          storeSupplier.serde, 
          storeSupplier.loggingEnabled, 
          config
        )
      )
    }
}
