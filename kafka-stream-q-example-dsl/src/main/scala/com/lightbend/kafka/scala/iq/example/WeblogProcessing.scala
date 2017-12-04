package com.lightbend.kafka.scala.iq.example

import java.io.{PrintWriter, StringWriter}
import java.lang.{Long => JLong}
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.concurrent.Executors

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import com.lightbend.kafka.scala.iq.http.{HttpRequester, KeyValueFetcher }
import com.lightbend.kafka.scala.iq.services.{ MetadataService, LocalStateStoreQuery }

import config.KStreamConfig._
import http.{ WeblogDSLHttpService, SummaryInfoFetcher }
import models.{LogParseUtil, LogRecord}

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.{ StreamsBuilder, Consumed }
import org.apache.kafka.streams.state.{ HostInfo, KeyValueStore, WindowStore }
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}

import com.lightbend.kafka.scala.streams._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object WeblogProcessing extends WeblogWorkflow {

  final val ACCESS_COUNT_PER_HOST_STORE = "access-count-per-host"
  final val PAYLOAD_SIZE_PER_HOST_STORE = "payload-size-per-host"
  final val WINDOWED_ACCESS_COUNT_PER_HOST_STORE = "windowed-access-count-per-host"
  final val WINDOWED_PAYLOAD_SIZE_PER_HOST_STORE = "windowed-payload-size-per-host"

  def main(args: Array[String]): Unit = workflow()

  override def startRestProxy(streams: KafkaStreams, hostInfo: HostInfo,
                              actorSystem: ActorSystem, materializer: ActorMaterializer): WeblogDSLHttpService = {

    implicit val system = actorSystem

    lazy val defaultParallelism: Int = {
      val rt = Runtime.getRuntime
      rt.availableProcessors() * 4
    }

    def defaultExecutionContext(parallelism: Int = defaultParallelism): ExecutionContext =
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism))

    val executionContext = defaultExecutionContext()

    // service for fetching metadata information
    val metadataService = new MetadataService(streams)

    // service for fetching from local state store
    val localStateStoreQuery = new LocalStateStoreQuery[String, Long]

    // http service for request handling
    val httpRequester = new HttpRequester(system, materializer, executionContext)

    val restService = new WeblogDSLHttpService(
      hostInfo,
      new SummaryInfoFetcher(new KeyValueFetcher(metadataService, localStateStoreQuery, httpRequester, streams, executionContext, hostInfo)),
      system, materializer, executionContext
    )
    restService.start()
    restService
  }

  override def createStreams(config: ConfigData): KafkaStreams = {
    // Kafka stream configuration
    val streamingConfig = {
      val settings = new Properties
      settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-weblog-processing")
      settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokers)

      config.schemaRegistryUrl.foreach{ url =>
        settings.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, url)
      }

      settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
      settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)

      // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
      // Note: To re-run the demo, you need to use the offset reset tool:
      // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
      settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      // need this for query service
      val endpointHostName = translateHostInterface(config.httpInterface)
      logger.info(s"Endpoint host name $endpointHostName")

      settings.put(StreamsConfig.APPLICATION_SERVER_CONFIG, s"$endpointHostName:${config.httpPort}")

      // default is /tmp/kafka-streams
      settings.put(StreamsConfig.STATE_DIR_CONFIG, config.stateStoreDir)

      // Set the commit interval to 500ms so that any changes are flushed frequently and the summary
      // data are updated with low latency.
      settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500")

      settings
    }

    implicit val builder = new StreamsBuilderS

    generateLogRecords(config)

    //
    // assumption : the topic contains serialized records of LogRecord (serialized through logRecordSerde)
    val logRecords = 
      builder.stream(List(config.toTopic.get), Consumed.`with`(byteArraySerde, logRecordSerde))

    generateAvro(logRecords, config)
    hostCountSummary(logRecords, config)
    totalPayloadPerHostSummary(logRecords, config)

    new KafkaStreams(builder.build(), streamingConfig)
  }

  /**
    * Clean and format input data.  Redirect records that cause a parsing error to the error topic.
    */
  def generateLogRecords(config: ConfigData)(implicit builder: StreamsBuilderS): Unit = {

    // will read network data from `fromTopic`
    val logs = builder.stream[Array[Byte], String](config.fromTopic)

    def predicateValid: (Array[Byte], Extracted) => Boolean = { (_, value) =>
      value match {
        case ValidLogRecord(_) => true
        case _ => false
      }
    }

    def predicateError: (Array[Byte], Extracted) => Boolean = { (_, value) =>
      value match {
        case ValueError(_, _) => true
        case _ => false
      }
    }

    // extract values after transformation
    val filtered = logs.mapValues { record =>
      LogParseUtil.parseLine(record) match {
        case Success(r) => ValidLogRecord(r)
        case Failure(ex) => ValueError(ex, record)
      }
    }.branch(predicateValid, predicateError)

    // push the labelled data
    filtered(0).mapValues {
      case ValidLogRecord(r) => r
      case _ => ??? // should never happen since we pre-emptively filtered with `branch`
    }.to(config.toTopic.get, Produced.`with`(byteArraySerde, logRecordSerde))

    // push the extraction errors
    filtered(1).mapValues {
      case ValueError(_, v) =>
        val writer = new StringWriter()
        (writer.toString, v)
      case _ => ??? // should never happen since we pre-emptively filtered with `branch`
    }.to(config.errorTopic, Produced.`with`(byteArraySerde, tuple2StringSerde))
  }

  sealed abstract class Extracted { }
  final case class ValidLogRecord(record: LogRecord) extends Extracted
  final case class ValueError(exception: Throwable, originalRecord: String) extends Extracted

  def generateAvro(logRecords: KStreamS[Array[Byte], LogRecord], config: ConfigData): Unit = {
    logRecords.mapValues(makeAvro)
      .to(config.avroTopic.get, Produced.`with`(byteArraySerde, logRecordAvroSerde(config.schemaRegistryUrl)))
  }

  /**
    * Transform a LogRecord into an Avro SpecificRecord, LogRecordAvro, generated by the Avro compiler
    */
  def makeAvro(record: LogRecord): LogRecordAvro =
    LogRecordAvro.newBuilder()
      .setHost(record.host)
      .setClientId(record.clientId)
      .setUser(record.user)
      .setTimestamp(record.timestamp.format(DateTimeFormatter.ofPattern("yyyy MM dd")))
      .setMethod(record.method)
      .setEndpoint(record.endpoint)
      .setProtocol(record.protocol)
      .setHttpReplyCode(record.httpReplyCode)
      .setPayloadSize(record.payloadSize)
      .build()

  /**
    * Summary count of number of times each host has been accessed
    */
  def hostCountSummary(logRecords: KStreamS[Array[Byte], LogRecord], config: ConfigData)(implicit builder: StreamsBuilderS): Unit = {

    val groupedStream =
      logRecords.mapValues(_.host)
        .map((_, value) => (value, value))
        .groupByKey(Serialized.`with`(stringSerde, stringSerde))
    
    // since this is a KTable (changelog stream), only the latest summarized information
    // for a host will be the correct one - all earlier records will be considered out of date
    //
    // materialize the summarized information into a topic
    groupedStream.count(ACCESS_COUNT_PER_HOST_STORE, Some(stringSerde))
      .toStream.to(config.summaryAccessTopic.get, Produced.`with`(stringSerde, longSerde))

    groupedStream.windowedBy(TimeWindows.of(60000))
      .count(WINDOWED_ACCESS_COUNT_PER_HOST_STORE, Some(stringSerde))
      .toStream.to(config.windowedSummaryAccessTopic.get, Produced.`with`(windowedStringSerde, longSerde))

    // print the topic info (for debugging)
    builder.stream(List(config.summaryAccessTopic.get), Consumed.`with`(stringSerde, longSerde))
      .print(Printed.toSysOut[String, Long].withKeyValueMapper { new KeyValueMapper[String, Long, String]() {
        def apply(key: String, value: Long) = s"""$key / $value"""
      }})
  }

  /**
    * Aggregate value of payloadSize per host
    */
  def totalPayloadPerHostSummary(logRecords: KStreamS[Array[Byte], LogRecord], config: ConfigData)(implicit builder: StreamsBuilderS): Unit = {
    val groupedStream =
      logRecords.mapValues(record => (record.host, record.payloadSize))
        .map { case (_, (host, size)) => (host, size) }
        .groupByKey(Serialized.`with`(stringSerde, longSerde))

    // materialize the summarized information into a topic
    groupedStream
      .aggregate(
        () => 0L,
        (_: String, s: Long, agg: Long) => s + agg,
        Materialized.as(PAYLOAD_SIZE_PER_HOST_STORE)
          .withKeySerde(stringSerde)
          .withValueSerde(longSerde)
      )
      .toStream.to(config.summaryPayloadTopic.get, Produced.`with`(stringSerde, longSerde))

    groupedStream
      .windowedBy(TimeWindows.of(60000))
      .aggregate(
        () => 0L,
        (_: String, s: Long, agg: Long) => s + agg,
        Materialized.as(WINDOWED_PAYLOAD_SIZE_PER_HOST_STORE)
          .withKeySerde(stringSerde)
          .withValueSerde(longSerde)
      )
      .toStream.to(config.windowedSummaryPayloadTopic.get, Produced.`with`(windowedStringSerde, longSerde))

    builder.stream(List(config.summaryPayloadTopic.get), Consumed.`with`(stringSerde, longSerde))
      .print(Printed.toSysOut[String, Long].withKeyValueMapper { new KeyValueMapper[String, Long, String]() {
        def apply(key: String, value: Long) = s"""$key / $value"""
      }})
  }
}
