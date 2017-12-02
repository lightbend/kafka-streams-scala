package com.lightbend.kafka.scala.iq.example
package config

import cats._
import cats.data._
import cats.instances.all._

import scala.util.Try
import com.typesafe.config.Config
import scala.concurrent.duration._


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
    brokers: String, 
    zk: String, 
    schemaRegistryUrl: Option[String],
    stateStoreDir: String
  )

  private[KStreamConfig] case class TopicSettings(
    fromTopic: String, 
    errorTopic: String,
    toTopic: Option[String], 
    avroTopic: Option[String], 
    summaryAccessTopic: Option[String], 
    windowedSummaryAccessTopic: Option[String], 
    summaryPayloadTopic: Option[String], 
    windowedSummaryPayloadTopic: Option[String]
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
    def brokers = ks.serverSettings.brokers
    def zk = ks.serverSettings.zk
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
      KafkaSettings(
        ServerSettings(
          config.getString("dcos.kafka.brokers"),
          config.getString("dcos.kafka.zookeeper"),
          getStringMaybe(config, "dcos.kafka.schemaregistryurl"),
          config.getString("dcos.kafka.statestoredir")
        ),
        TopicSettings(
          config.getString("dcos.kafka.fromtopic"),
          config.getString("dcos.kafka.errortopic"),
          getStringMaybe(config, "dcos.kafka.totopic"),
          getStringMaybe(config, "dcos.kafka.avrotopic"),
          getStringMaybe(config, "dcos.kafka.summaryaccesstopic"),
          getStringMaybe(config, "dcos.kafka.windowedsummaryaccesstopic"),
          getStringMaybe(config, "dcos.kafka.summarypayloadtopic"),
          getStringMaybe(config, "dcos.kafka.windowedsummarypayloadtopic")
        )
      )
    }
  }

  private def fromHttpConfig: ConfigReader[HttpSettings] = Kleisli { (config: Config) =>
    Try {
      HttpSettings(
        config.getString("dcos.http.interface"),
        config.getInt("dcos.http.port")
      )
    }
  }

  private def fromDataLoaderConfig: ConfigReader[DataLoaderSettings] = Kleisli { (config: Config) =>
    Try {
      DataLoaderSettings(
        config.getString("dcos.kafka.loader.sourcetopic"),
        getStringMaybe(config, "dcos.kafka.loader.directorytowatch"),
        config.getDuration("dcos.kafka.loader.pollinterval")
      )
    }
  }

  def fromConfig: ConfigReader[ConfigData] = for {
    k <- fromKafkaConfig
    h <- fromHttpConfig
    d <- fromDataLoaderConfig
  } yield ConfigData(k, h, d)
}

