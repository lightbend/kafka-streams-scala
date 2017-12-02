package com.lightbend.kafka.scala.iq.example
package http

import akka.actor.ActorSystem

import akka.stream.ActorMaterializer

import io.circe.generic.auto._
import io.circe.syntax._

import org.apache.kafka.streams.state.HostInfo

import scala.concurrent.ExecutionContext
import com.lightbend.kafka.scala.iq.http.{ InteractiveQueryHttpService, KeyValueFetcher }


class WeblogDSLHttpService(
  hostInfo: HostInfo, 
  summaryInfoFetcher: SummaryInfoFetcher,
  actorSystem: ActorSystem,
  actorMaterializer: ActorMaterializer,
  ec: ExecutionContext
) extends InteractiveQueryHttpService(hostInfo, actorSystem, actorMaterializer, ec) { 


  // define the routes
  val routes = handleExceptions(myExceptionHandler) {
    pathPrefix("weblog") {
      (get & pathPrefix("access" / "win") & path(Segment)) { hostKey =>
        complete {
          summaryInfoFetcher.fetchWindowedAccessCountSummary(hostKey, 0, System.currentTimeMillis).map(_.asJson)
        }
      } ~
      (get & pathPrefix("bytes" / "win") & path(Segment)) { hostKey =>
        complete {
          summaryInfoFetcher.fetchWindowedPayloadSizeSummary(hostKey, 0, System.currentTimeMillis).map(_.asJson)
        }
      } ~
      (get & pathPrefix("access") & path(Segment)) { hostKey =>
        complete {
          summaryInfoFetcher.fetchAccessCountSummary(hostKey).map(_.asJson)
        }
      } ~
      (get & pathPrefix("bytes") & path(Segment)) { hostKey =>
        complete {
          summaryInfoFetcher.fetchPayloadSizeSummary(hostKey).map(_.asJson)
        }
      }
    }
  }
}
