package com.lightbend.kafka.scala.iq.example
package processor

import com.twitter.algebird.{BloomFilterMonoid, BF, Hash128, Approximate}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}
import org.apache.kafka.streams.state.StateSerdes
import org.apache.kafka.streams.processor.internals.{ProcessorStateManager, RecordCollector}

/**
 * Bloom Filter as a StateStore. The only query it supports is membership.
 */ 
class BFStore[T: Hash128](override val name: String,
                          val loggingEnabled: Boolean = true,
                          val numHashes: Int = 6,
                          val width: Int = 32,
                          val seed: Int = 1) extends WriteableBFStore[T] with StateStore {

  private val bfMonoid = new BloomFilterMonoid[T](numHashes, width)

  /**
    * The "storage backend" of this store.
    *
    * Needs proper initializing in case the store's changelog is empty.
    */
  private[processor] var bf: BF[T] = bfMonoid.zero

  private[processor] var changeLogger: BFStoreChangeLogger[Integer, BF[T]] = _

  private[processor] val changelogKey = 42
  private final val ACCEPTABLE_PROBABILITY = 0.75

  private[processor] def bfFrom(items: Seq[T]): BF[T] = bfMonoid.create(items:_*)

  private[processor] def bfFrom(item: T): BF[T] = bfMonoid.create(item)

  @volatile private var open: Boolean = false

  /**
    * Initializes this store, including restoring the store's state from its changelog.
    */
  override def init(context: ProcessorContext, root: StateStore): Unit = {
    val serdes = new StateSerdes[Integer, BF[T]](
      name,
      Serdes.Integer(),
      BFSerde[T])

    changeLogger = new BFStoreChangeLogger[Integer, BF[T]](name, context, serdes)

    // Note: We must manually guard with `loggingEnabled` here because `context.register()` ignores
    // that parameter.
    if (root != null && loggingEnabled) {
      context.register(root, loggingEnabled, (_, value) => {
        if (value == null) {
          bf = bfMonoid.zero
        }
        else {
          bf = serdes.valueFrom(value)
        }
      })
    }

    open = true
  }

  def +(item: T): Unit = bf = bf + item

  def contains(item: T): Boolean = {
    val v = bf.contains(item)
    v.isTrue && v.withProb > ACCEPTABLE_PROBABILITY
  }

  def maybeContains(item: T): Boolean = bf.maybeContains(item)
  def size: Approximate[Long] = bf.size


  override val persistent: Boolean = false

  override def isOpen: Boolean = open

  /**
    * Periodically saves the latest BF state to Kafka.
    *
    * =Implementation detail=
    *
    * The changelog records have the form: (hardcodedKey, BF).  That is, we are backing up the
    * underlying CMS data structure in its entirety to Kafka.
    */
  override def flush(): Unit = {
    // if (loggingEnabled) {
      // changeLogger.logChange(changelogKey, bf)
    // }
  }

  override def close(): Unit = {
    open = false
  }

  override def read(value: T): Boolean = contains(value)

  override def write(value: T): Unit = this + value

}

abstract class ReadableBFStore[T: Hash128] {
  def read(value: T): Boolean
}

abstract class WriteableBFStore[T: Hash128] extends ReadableBFStore[T] {
  def write(value: T): Unit
}
