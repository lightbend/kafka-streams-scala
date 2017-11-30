package com.lightbend.kafka.scala.iq
package http

import akka.stream.ActorMaterializer
import akka.actor.ActorSystem

import org.apache.kafka.streams.{ KafkaStreams }
import org.apache.kafka.streams.state.HostInfo
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.{ Future, ExecutionContext}
import scala.util.{ Success, Failure }

import com.typesafe.scalalogging.LazyLogging
import services.{ MetadataService, HostStoreInfo, LocalStateStoreQuery }
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import serializers.Serializers

class KeyValueFetcher(
  metadataService: MetadataService, 
  localStateStoreQuery: LocalStateStoreQuery[String, Long],
  httpRequester: HttpRequester, 
  streams: KafkaStreams, 
  executionContext: ExecutionContext, 
  hostInfo: HostInfo)(implicit actorSystem: ActorSystem) extends LazyLogging with FailFastCirceSupport with Serializers {

  private implicit val ec: ExecutionContext = executionContext

  def fetch(key: String, store: String, path: String): Future[Long] = { 

    metadataService.streamsMetadataForStoreAndKey(store, key, stringSerializer) match {
      case Success(host) => {
        // key is on another instance. call the other instance to fetch the data.
        if (!thisHost(host)) {
          logger.warn(s"Key $key is on another instance not on $host - requerying ..")
          httpRequester.queryFromHost[Long](host, path)
        } else {
          // key is on this instance
          localStateStoreQuery.queryStateStore(streams, store, key)
        }
      }
      case Failure(ex) => Future.failed(ex)
    }
  }

  def fetchWindowed(key: String, store: String, path: String, 
    fromTime: Long, toTime: Long): Future[List[(Long, Long)]] = 

    metadataService.streamsMetadataForStoreAndKey(store, key, stringSerializer) match {
      case Success(host) => {
        // key is on another instance. call the other instance to fetch the data.
        if (!thisHost(host)) {
          logger.warn(s"Key $key is on another instance not on $host - requerying ..")
          httpRequester.queryFromHost[List[(Long, Long)]](host, path)
        } else {
          // key is on this instance
          localStateStoreQuery.queryWindowedStateStore(streams, store, key, fromTime, toTime)
        }
      }
      case Failure(ex) => Future.failed(ex)
    }

  private def thisHost(host: HostStoreInfo): Boolean = 
    host.host.equals(translateHostInterface(hostInfo.host)) && host.port == hostInfo.port
}

