package com.lightbend.kafka.scala.iq.example
package config

import cats._
import cats.data._
import cats.instances.all._

import scala.util.Try
import com.typesafe.config.Config
import scala.concurrent.duration._
import com.lightbend.kafka.scala.server._


/**
 * This object wraps the native Java config APIs into a monadic
 * interpreter
 */ 
object KStreamConfig {

  private[KStreamConfig] case class KafkaSettings(
    serverSettings: ServerSettings,
    topicSettings: TopicSettings
  )

  private[KStreamConfig] case class ServerSettings(
    localServer: Boolean,
    brokers: String, 
    schemaRegistryUrl: Option[String],
    stateStoreDir: String
  )

  private[KStreamConfig] case class TopicSettings(
    fromTopic: String, 
    errorTopic: String,
    toTopic: String, 
    avroTopic: String, 
    summaryAccessTopic: String, 
    windowedSummaryAccessTopic: String, 
    summaryPayloadTopic: String, 
    windowedSummaryPayloadTopic: String
  )

  private[KStreamConfig] case class HttpSettings(
    interface: String,
    port: Int
  )

  private[KStreamConfig] case class DataLoaderSettings(
    sourceTopic: String,
    directoryToWatch: Option[String],
    pollInterval: FiniteDuration
  )

  case class ConfigData(ks: KafkaSettings, hs: HttpSettings, dls: DataLoaderSettings) {
    def localServer = ks.serverSettings.localServer
    def brokers = ks.serverSettings.brokers
    def schemaRegistryUrl = ks.serverSettings.schemaRegistryUrl
    def fromTopic = ks.topicSettings.fromTopic
    def toTopic = ks.topicSettings.toTopic
    def avroTopic = ks.topicSettings.avroTopic
    def summaryAccessTopic = ks.topicSettings.summaryAccessTopic
    def windowedSummaryAccessTopic = ks.topicSettings.windowedSummaryAccessTopic
    def summaryPayloadTopic = ks.topicSettings.summaryPayloadTopic
    def windowedSummaryPayloadTopic = ks.topicSettings.windowedSummaryPayloadTopic
    def errorTopic = ks.topicSettings.errorTopic
    def stateStoreDir = ks.serverSettings.stateStoreDir
    def httpInterface = hs.interface
    def httpPort = hs.port
    def sourceTopic = dls.sourceTopic
    def directoryToWatch = dls.directoryToWatch
    def pollInterval = dls.pollInterval
  }

  type ConfigReader[A] = ReaderT[Try, Config, A]

  private def getStringMaybe(config: Config, key: String): Option[String] = try {
    val str = config.getString(key)
    if (str.trim.isEmpty) None else Some(str)
  } catch {
    case _: Exception => None
  }

  private def fromKafkaConfig: ConfigReader[KafkaSettings] = Kleisli { (config: Config) =>
    Try {
      val local = config.getBoolean("kafka.localserver")
      val serverSettings = 
        if (local) {
          ServerSettings(
            local,
            s"localhost:${KafkaLocalServer.DefaultPort}",
            getStringMaybe(config, "kafka.schemaregistryurl"),
            config.getString("kafka.statestoredir")
          )
        } else {
          ServerSettings(
            local,
            config.getString("kafka.brokers"),
            getStringMaybe(config, "kafka.schemaregistryurl"),
            config.getString("kafka.statestoredir")
          )
        }

      KafkaSettings(
        serverSettings,
        TopicSettings(
          config.getString("kafka.fromtopic"),
          config.getString("kafka.errortopic"),
          config.getString("kafka.totopic"),
          config.getString("kafka.avrotopic"),
          config.getString("kafka.summaryaccesstopic"),
          config.getString("kafka.windowedsummaryaccesstopic"),
          config.getString("kafka.summarypayloadtopic"),
          config.getString("kafka.windowedsummarypayloadtopic")
        )
      )
    }
  }

  private def fromHttpConfig: ConfigReader[HttpSettings] = Kleisli { (config: Config) =>
    Try {
      HttpSettings(
        config.getString("http.interface"),
        config.getInt("http.port")
      )
    }
  }

  private def fromDataLoaderConfig: ConfigReader[DataLoaderSettings] = Kleisli { (config: Config) =>
    Try {
      DataLoaderSettings(
        config.getString("kafka.loader.sourcetopic"),
        getStringMaybe(config, "kafka.loader.directorytowatch"),
        config.getDuration("kafka.loader.pollinterval")
      )
    }
  }

  def fromConfig: ConfigReader[ConfigData] = for {
    k <- fromKafkaConfig
    h <- fromHttpConfig
    d <- fromDataLoaderConfig
  } yield ConfigData(k, h, d)
}

