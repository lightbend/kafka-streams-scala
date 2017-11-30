import sbt._
import Versions._

object Dependencies {

  implicit class Exclude(module: ModuleID) {
    def log4jExclude: ModuleID =
      module excludeAll(ExclusionRule("log4j"))

    def driverExclusions: ModuleID =
      module.log4jExclude.exclude("com.google.guava", "guava")
        .excludeAll(ExclusionRule("org.slf4j"))
  }

  val kafkaStreams    = "org.apache.kafka"                % "kafka-streams"     % kafkaVersion
  val scalaLogging    = "com.typesafe.scala-logging"     %% "scala-logging"     % scalaLoggingVersion
  val logback         = "ch.qos.logback"                  % "logback-classic"   % logbackVersion
  val kafka           = "org.apache.kafka"                % "kafka_2.12"        % kafkaVersion
  val curator         = "org.apache.curator"              % "curator-test"      % curatorVersion
  val minitest        = "io.monix"                       %% "minitest"          % minitestVersion
  val minitestLaws    = "io.monix"                       %% "minitest-laws"     % minitestVersion
  val algebird        = "com.twitter"                    %% "algebird-core"     % algebirdVersion 
  val chill           = "com.twitter"                    %% "chill"             % chillVersion 
  val circeCore       = "io.circe"                       %% "circe-core"        % circeVersion
  val circeGeneric    = "io.circe"                       %% "circe-generic"     % circeVersion
  val circeParser     = "io.circe"                       %% "circe-parser"      % circeVersion
  val akkaSlf4j       = "com.typesafe.akka"              %% "akka-slf4j"        % akkaVersion
  val akkaStreams     = "com.typesafe.akka"              %% "akka-stream"       % akkaVersion
  val akkaHttp        = "com.typesafe.akka"              %% "akka-http"         % akkaHttpVersion
  val akkaHttpCirce   = "de.heikoseeberger"              %% "akka-http-circe"   % akkaHttpCirceVersion
  val bijection       = "com.twitter"                    %% "bijection-avro"    % bijectionVersion
}



