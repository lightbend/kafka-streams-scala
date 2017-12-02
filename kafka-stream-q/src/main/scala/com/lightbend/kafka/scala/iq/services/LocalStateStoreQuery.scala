package com.lightbend.kafka.scala.iq
package services

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{ ReadOnlyKeyValueStore, QueryableStoreTypes, QueryableStoreType, ReadOnlyWindowStore }

import scala.collection.JavaConverters._
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._
import scala.util.{ Try, Success, Failure }
import com.typesafe.scalalogging.LazyLogging
import akka.actor.ActorSystem

class LocalStateStoreQuery[K, V] extends LazyLogging {

  final val maxRetryCount = 10
  final val delayBetweenRetries = 1.second

  /**
   * For all the following query methods, we need to implement a retry semantics when we invoke
   * `streams.store()`. This is because if the application is run in a distributed mode (multiple
   * instances), this function call can throw `InvalidStateStoreException` if state stores are being
   * migrated when the call is made. And migration is done when new instances of the application come up
   * or Kafka Streams does a rebalancing.
   *
   * In such cases we need to retry till the rebalancing is complete or we run out of retry count.
   */ 
  def queryStateStore(streams: KafkaStreams, store: String, key: K)
    (implicit ex: ExecutionContext, as: ActorSystem): Future[V] = {

    val q: QueryableStoreType[ReadOnlyKeyValueStore[K, V]] = QueryableStoreTypes.keyValueStore()
    retry(streams.store(store, q), delayBetweenRetries, maxRetryCount)(ex, as.scheduler).map(_.get(key))
  }

  def queryWindowedStateStore(streams: KafkaStreams, store: String, key: K, fromTime: Long, toTime: Long)
    (implicit ex: ExecutionContext, as: ActorSystem): Future[List[(Long, V)]] = {

    val q: QueryableStoreType[ReadOnlyWindowStore[K, V]] = QueryableStoreTypes.windowStore()

    retry(streams.store(store, q), delayBetweenRetries, maxRetryCount)(ex, as.scheduler).map(
      _.fetch(key, fromTime, toTime)
       .asScala
       .toList
       .map(kv => (Long2long(kv.key), kv.value)))
  }
}
