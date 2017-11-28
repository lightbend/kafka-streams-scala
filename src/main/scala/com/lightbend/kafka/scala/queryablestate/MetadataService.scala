package com.lightbend.kafka.scala.queryablestate

/**
  * Looks up StreamsMetadata from KafkaStreams and converts the results
  * into Beans that can be JSON serialized
  * https://github.com/confluentinc/examples/blob/3.2.x/kafka-streams/src/main/java/io/confluent/examples/streams/interactivequeries/MetadataService.java
  */

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{HostInfo, StreamsMetadata}
import java.net.InetAddress
import java.util

import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._


class MetadataService(val streams: KafkaStreams) {
  /**
    * Get the metadata for all of the instances of this Kafka Streams application
    *
    * @return List of { @link HostStoreInfo}
    */


  /**
    * Get the metadata for all of the instances of this Kafka Streams application
    *
    * @return List of { @link HostStoreInfo}
    */
  def streamsMetadataport(port : Int = 0): util.List[HostStoreInfo] = {
    val metadata = streams.allMetadata.toSeq
    mapInstancesToHostStoreInfo(metadata)
  }

  /**
    * Get the metadata for all instances of this Kafka Streams application that currently
    * has the provided store.
    *
    * @param store The store to locate
    * @return List of { @link HostStoreInfo}
    */
  def streamsMetadataForStore(store: String, port: Int = 0): util.List[HostStoreInfo] = {
    val metadata = streams.allMetadataForStore(store).toSeq match{
      case list if !list.isEmpty => list
      case _ => Seq(new StreamsMetadata(
        new HostInfo("localhost", port),
        new util.HashSet[String](util.Arrays.asList(store)), util.Collections.emptySet[TopicPartition]))
    }
    mapInstancesToHostStoreInfo(metadata)
  }

  private def mapInstancesToHostStoreInfo(metadatas: Seq[StreamsMetadata]) = metadatas.map(convertMetadata(_))


  private def convertMetadata(metadata: StreamsMetadata) : HostStoreInfo = {
    val currentHost = metadata.host match {
      case host if host.equalsIgnoreCase("localhost") => try {
        InetAddress.getLocalHost.getHostAddress
      }
      catch { case t: Throwable => "127.0.0.1"}
      case host => host
    }
    new HostStoreInfo(currentHost, metadata.port, metadata.stateStoreNames)
  }
}
