package com.lightbend.kafka.scala.iq.example
package ingestion

import java.nio.file.{ Path, FileSystems }

import akka.{ NotUsed, Done }
import akka.util.ByteString
import akka.actor.ActorSystem

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Framing, Source }
import akka.stream.alpakka.file.DirectoryChange._
import akka.stream.alpakka.file.scaladsl._

import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer

import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._
import scala.concurrent.Future

import config.KStreamConfig._
import serializers.AppSerializers
import com.typesafe.scalalogging.LazyLogging

object DataIngestion extends LazyLogging with AppSerializers {
  def registerForIngestion(config: ConfigData)
    (implicit system: ActorSystem, materializer: ActorMaterializer): Future[Done] = {

    val fs = FileSystems.getDefault

    config.directoryToWatch.map { dir =>
      DirectoryChangesSource(fs.getPath(dir),
        config.pollInterval, 
        maxBufferSize = 1024).runForeach {

        case (path, _@(Creation | Modification)) => {
          val _ = produce(path, config)
          ()
        }
        case (_, Deletion) => ()
      }
    }.getOrElse(Future.failed(new IllegalArgumentException("No directoryToWatch set in data ingestion module")))
  }
   
  private def produce(path: Path, config: ConfigData)
    (implicit system: ActorSystem, materializer: ActorMaterializer): NotUsed = {

    val MAX_CHUNK_SIZE = 25000
    val POLLING_INTERVAL = 250 millis

    val producerSettings = ProducerSettings(system, byteArraySerde.serializer, stringSerializer)
      .withBootstrapServers(config.brokers)
    
    val logLines: Source[String, NotUsed] =
      FileTailSource(path, MAX_CHUNK_SIZE, 0, POLLING_INTERVAL)
        .via(Framing.delimiter(ByteString.fromString("\n"), MAX_CHUNK_SIZE))
        .map(_.utf8String)

    logLines
      .map(new ProducerRecord[Array[Byte], String](config.sourceTopic, _))
      .to(Producer.plainSink(producerSettings))
      .run()
  }
}
