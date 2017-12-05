package com.lightbend.kafka.scala.iq.example
package processor

import org.apache.kafka.streams.state.QueryableStoreType
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.state.internals.StateStoreProvider

import com.twitter.algebird.{ Hash128, ApproximateBoolean }

import scala.collection.JavaConverters._

class BFStoreType[T: Hash128] extends QueryableStoreType[ReadableBFStore[T]] {
  def accepts(stateStore: StateStore) = stateStore.isInstanceOf[BFStore[T]]

  def create(storeProvider: StateStoreProvider, storeName: String): BFStoreTypeWrapper[T] = 
    new BFStoreTypeWrapper[T](storeProvider, storeName, this)
}

class BFStoreTypeWrapper[T: Hash128](val provider: StateStoreProvider, val storeName: String, 
  val bfStoreType: QueryableStoreType[ReadableBFStore[T]]) extends ReadableBFStore[T] {

  def read(value: T): Boolean = {
    val stores: List[ReadableBFStore[T]] = provider.stores(storeName, bfStoreType).asScala.toList
    stores.map(store => store.read(value)).exists(_ == true)
  }
}
