/**
  * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
  */
package com.lightbend.kafka.scala.server

// Loosely based on Lagom implementation at
//  https://github.com/lagom/lagom/blob/master/dev/kafka-server/src/main/scala/com/lightbend/lagom/internal/kafka/KafkaLocalServer.scala

import java.io.{File, IOException}
import java.util.Properties

import org.apache.curator.test.TestingServer
import com.typesafe.scalalogging.LazyLogging

import kafka.server.{KafkaConfig, KafkaServerStartable}

import scala.util.{Failure, Success, Try}

import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.utils.ZkUtils

class KafkaLocalServer private (kafkaProperties: Properties, zooKeeperServer: ZooKeeperLocalServer)
    extends LazyLogging {

  import KafkaLocalServer._

  private var broker = null.asInstanceOf[KafkaServerStartable] // scalastyle:ignore
  private var zkUtils: ZkUtils =
    ZkUtils.apply(s"localhost:${zooKeeperServer.getPort()}",
                  DEFAULT_ZK_SESSION_TIMEOUT_MS,
                  DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
                  false)

  def start(): Unit = {

    broker = KafkaServerStartable.fromProps(kafkaProperties)
    broker.startup()
  }

  //scalastyle:off null
  def stop(): Unit =
    if (broker != null) {
      broker.shutdown()
      zooKeeperServer.stop()
      broker = null.asInstanceOf[KafkaServerStartable]
    }
  //scalastyle:on null

  /**
    * Create a Kafka topic with 1 partition and a replication factor of 1.
    *
    * @param topic The name of the topic.
    */
  def createTopic(topic: String): Unit =
    createTopic(topic, 1, 1, new Properties)

  /**
    * Create a Kafka topic with the given parameters.
    *
    * @param topic       The name of the topic.
    * @param partitions  The number of partitions for this topic.
    * @param replication The replication factor for (the partitions of) this topic.
    */
  def createTopic(topic: String, partitions: Int, replication: Int): Unit =
    createTopic(topic, partitions, replication, new Properties)

  /**
    * Create a Kafka topic with the given parameters.
    *
    * @param topic       The name of the topic.
    * @param partitions  The number of partitions for this topic.
    * @param replication The replication factor for (partitions of) this topic.
    * @param topicConfig Additional topic-level configuration settings.
    */
  def createTopic(topic: String, partitions: Int, replication: Int, topicConfig: Properties): Unit =
    AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig, RackAwareMode.Enforced)

  def deleteTopic(topic: String): Unit = AdminUtils.deleteTopic(zkUtils, topic)
}

import Utils._

object KafkaLocalServer extends LazyLogging {
  final val DefaultPort                        = 9092
  final val DefaultResetOnStart                = true
  private val DEFAULT_ZK_CONNECT               = "localhost:2181"
  private val DEFAULT_ZK_SESSION_TIMEOUT_MS    = 10 * 1000
  private val DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 8 * 1000

  final val basDir = "tmp/"

  final private val kafkaDataFolderName = "kafka_data"

  def apply(cleanOnStart: Boolean, localStateDir: Option[String] = None): KafkaLocalServer =
    this(DefaultPort, ZooKeeperLocalServer.DefaultPort, cleanOnStart, localStateDir)

  def apply(kafkaPort: Int,
            zookeeperServerPort: Int,
            cleanOnStart: Boolean,
            localStateDir: Option[String]): KafkaLocalServer = {

    // delete kafka data dir on clean start
    val kafkaDataDir: File = (for {
      kdir <- dataDirectory(basDir, kafkaDataFolderName)
      _    <- if (cleanOnStart) deleteDirectory(kdir) else Try(())
    } yield kdir) match {
      case Success(d)  => d
      case Failure(ex) => throw ex
    }

    // delete kafka local state dir on clean start
    localStateDir.foreach { d =>
      for {
        kdir <- dataDirectory("", d)
        _    <- if (cleanOnStart) deleteDirectory(kdir) else Try(())
      } yield (())
    }

    logger.info(s"Kafka data directory is $kafkaDataDir.")

    val kafkaProperties = createKafkaProperties(kafkaPort, zookeeperServerPort, kafkaDataDir)

    val zk = new ZooKeeperLocalServer(zookeeperServerPort, cleanOnStart)
    zk.start()
    new KafkaLocalServer(kafkaProperties, zk)
  }

  /**
    * Creates a Properties instance for Kafka customized with values passed in argument.
    */
  private def createKafkaProperties(kafkaPort: Int, zookeeperServerPort: Int, dataDir: File): Properties = {

    // TODO: Probably should be externalized into properties. Was rushing this in
    val kafkaProperties = new Properties
    kafkaProperties.put(KafkaConfig.ListenersProp, s"PLAINTEXT://localhost:$kafkaPort")
    kafkaProperties.put(KafkaConfig.ZkConnectProp, s"localhost:$zookeeperServerPort")
    kafkaProperties.put(KafkaConfig.ZkConnectionTimeoutMsProp, "6000")
    kafkaProperties.put(KafkaConfig.BrokerIdProp, "0")
    kafkaProperties.put(KafkaConfig.NumNetworkThreadsProp, "3")
    kafkaProperties.put(KafkaConfig.NumIoThreadsProp, "8")
    kafkaProperties.put(KafkaConfig.SocketSendBufferBytesProp, "102400")
    kafkaProperties.put(KafkaConfig.SocketReceiveBufferBytesProp, "102400")
    kafkaProperties.put(KafkaConfig.SocketRequestMaxBytesProp, "104857600")
    kafkaProperties.put(KafkaConfig.NumPartitionsProp, "1")
    kafkaProperties.put(KafkaConfig.NumRecoveryThreadsPerDataDirProp, "1")
    kafkaProperties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
    kafkaProperties.put(KafkaConfig.TransactionsTopicReplicationFactorProp, "1")
    kafkaProperties.put(KafkaConfig.LogRetentionTimeHoursProp, "2")
    kafkaProperties.put(KafkaConfig.LogSegmentBytesProp, "1073741824")
    kafkaProperties.put(KafkaConfig.LogCleanupIntervalMsProp, "300000")
    kafkaProperties.put(KafkaConfig.AutoCreateTopicsEnableProp, "true")
    kafkaProperties.put(KafkaConfig.ControlledShutdownEnableProp, "true")
    kafkaProperties.put(KafkaConfig.LogDirProp, dataDir.getAbsolutePath)

    kafkaProperties
  }
}

private class ZooKeeperLocalServer(port: Int, cleanOnStart: Boolean) extends LazyLogging {

  import KafkaLocalServer._
  import ZooKeeperLocalServer._

  private var zooKeeper = null.asInstanceOf[TestingServer] // scalastyle:ignore

  def start(): Unit = {
    // delete kafka data dir on clean start
    val zookeeperDataDir: File = (for {
      zdir <- dataDirectory(basDir, zookeeperDataFolderName)
      _    <- if (cleanOnStart) deleteDirectory(zdir) else Try(())
    } yield zdir) match {
      case Success(d)  => d
      case Failure(ex) => throw ex
    }
    logger.info(s"Zookeeper data directory is $zookeeperDataDir.")

    zooKeeper = new TestingServer(port, zookeeperDataDir, false)

    zooKeeper.start() // blocking operation
  }

  // scalastyle:off null
  def stop(): Unit =
    if (zooKeeper != null)
      try {
        zooKeeper.stop()
        zooKeeper = null.asInstanceOf[TestingServer]
      } catch {
        case _: IOException => () // nothing to do if an exception is thrown while shutting down
      }
  //scalastyle:on null

  def getPort(): Int = port
}

object ZooKeeperLocalServer {
  final val DefaultPort                     = 2181
  final private val zookeeperDataFolderName = "zookeeper_data"
}
