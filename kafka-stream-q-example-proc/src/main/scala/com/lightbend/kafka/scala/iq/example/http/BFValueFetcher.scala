package com.lightbend.kafka.scala.iq.example
package http

import akka.stream.ActorMaterializer
import akka.actor.ActorSystem

import org.apache.kafka.streams.{ KafkaStreams }
import org.apache.kafka.streams.state.HostInfo
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.{ Future, ExecutionContext}
import scala.util.{ Success, Failure }

import com.typesafe.scalalogging.LazyLogging
import com.lightbend.kafka.scala.iq.services.{ MetadataService, HostStoreInfo }
import services.AppStateStoreQuery
import com.lightbend.kafka.scala.iq.http.HttpRequester
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import serializers.AppSerializers

class BFValueFetcher(
  metadataService: MetadataService, 
  localStateStoreQuery: AppStateStoreQuery[String, Long],
  httpRequester: HttpRequester, 
  streams: KafkaStreams, 
  executionContext: ExecutionContext, 
  hostInfo: HostInfo)(implicit actorSystem: ActorSystem) extends LazyLogging with FailFastCirceSupport with AppSerializers {

  private implicit val ec: ExecutionContext = executionContext

  def checkIfPresent(hostKey: String): Future[Boolean] = {

    val store = WeblogDriver.LOG_COUNT_STATE_STORE
    val path = s"/weblog/access/check/$hostKey"

    metadataService.streamsMetadataForStoreAndKey(store, hostKey, stringSerializer) match {
      case Success(host) => {
        // hostKey is on another instance. call the other instance to fetch the data.
        if (!thisHost(host)) {
          logger.warn(s"Key $hostKey is on another instance not on ${translateHostInterface(hostInfo.host)}:${hostInfo.port} - requerying ..")
          httpRequester.queryFromHost[Boolean](host, path)
        } else {
          // hostKey is on this instance
          localStateStoreQuery.queryBFStore(streams, store, hostKey)
        }
      }
      case Failure(ex) => Future.failed(ex)
    }
  }

  private def thisHost(host: HostStoreInfo): Boolean =
    host.host.equals(translateHostInterface(hostInfo.host)) && host.port == hostInfo.port
}


