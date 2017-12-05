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
    stateStoreDir: String
  )

  private[KStreamConfig] case class TopicSettings(
    fromTopic: String, 
    errorTopic: String
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
    def fromTopic = ks.topicSettings.fromTopic
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
          config.getString("dcos.kafka.statestoredir")
        ),
        TopicSettings(
          config.getString("dcos.kafka.fromtopic"),
          config.getString("dcos.kafka.errortopic")
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

