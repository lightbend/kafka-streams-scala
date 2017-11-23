package com.lightbend.kafka.scala.queryablestate

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import org.apache.kafka.streams.KafkaStreams

import scala.concurrent.duration._

object StateServer {

  def starServer(streams: KafkaStreams, port: Int) = {

    implicit val system = ActorSystem("QueryableState")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher
    implicit val timeout = Timeout(10 seconds)
    val host = "127.0.0.1"
    val port = 8888
    val routes: Route = QueriesResource.storeRoutes(streams, port)

    Http().bindAndHandle(routes, host, port) map
      { binding => println(s"Starting models observer on port ${binding.localAddress}") } recover {
      case ex =>
        println(s"Models observer could not bind to $host:$port", ex.getMessage)
    }
  }
}
