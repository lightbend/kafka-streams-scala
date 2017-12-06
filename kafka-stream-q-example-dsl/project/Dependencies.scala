import sbt._
import Versions._

object Dependencies {

  val ks              = "com.lightbend"                %% "kafka-streams-scala"      % ksVersion exclude("org.slf4j", "slf4j-log4j12")
  val kq              = "com.lightbend"                %% "kafka-streams-query"      % kqVersion exclude("org.slf4j", "slf4j-log4j12")
  val algebird        = "com.twitter"                  %% "algebird-core"            % algebirdVersion 
  val chill           = "com.twitter"                  %% "chill"                    % chillVersion 
  val bijection       = "com.twitter"                  %% "bijection-avro"           % bijectionVersion
  val alpakka         = "com.lightbend.akka"           %% "akka-stream-alpakka-file" % alpakkaFileVersion
  val reactiveKafka   = "com.typesafe.akka"            %% "akka-stream-kafka"        % reactiveKafkaVersion
  val confluentAvro   = "io.confluent"                  % "kafka-avro-serializer"    % confluentPlatformVersion exclude("org.slf4j", "slf4j-log4j12")
  val akkaSlf4j       = "com.typesafe.akka"            %% "akka-slf4j"               % akkaVersion
  val akkaStreams     = "com.typesafe.akka"            %% "akka-stream"              % akkaVersion
  val akkaHttp        = "com.typesafe.akka"            %% "akka-http"                % akkaHttpVersion
  val akkaHttpCirce   = "de.heikoseeberger"            %% "akka-http-circe"          % akkaHttpCirceVersion
  val circeCore       = "io.circe"                     %% "circe-core"               % circeVersion
  val circeGeneric    = "io.circe"                     %% "circe-generic"            % circeVersion
  val circeParser     = "io.circe"                     %% "circe-parser"             % circeVersion
  val logback         = "ch.qos.logback"                % "logback-classic"          % logbackVersion
  val scalaLogging    = "com.typesafe.scala-logging"   %% "scala-logging"            % scalaLoggingVersion
}




