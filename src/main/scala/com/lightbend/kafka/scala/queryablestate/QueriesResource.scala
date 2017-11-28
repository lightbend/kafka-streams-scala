package com.lightbend.kafka.scala.queryablestate

// Need to be extended to support additional URLS

import javax.ws.rs.NotFoundException

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import de.heikoseeberger.akkahttpjackson.JacksonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.QueryableStoreTypes

object QueriesResource extends JacksonSupport {


  def storeRoutes(streams: KafkaStreams, port : Int): Route = {
    val metadataService = new MetadataService(streams)
    get {
      pathPrefix("state") {
        // TODO: need to add store name
        path("instances") {
          complete(
            metadataService.streamsMetadataForStore("", port)
          )
        } ~
          path("value") {
            // TODO: need to add store name and parameter
            val store = streams.store(ApplicationKafkaParameters.STORE_NAME, QueryableStoreTypes.keyValueStore[Integer,StoreState])
            if (store == null) throw new NoSuchElementException
            complete(store.get(STORE_ID).currentState)
          }
      }
    }
  }
}
