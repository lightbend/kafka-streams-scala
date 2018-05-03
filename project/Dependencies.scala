import sbt._
import Versions._

object Dependencies {

  implicit class Exclude(module: ModuleID) {
    def log4jExclude: ModuleID =
      module.excludeAll(ExclusionRule("log4j"))

    def driverExclusions: ModuleID =
      module.log4jExclude
        .exclude("com.google.guava", "guava")
        .excludeAll(ExclusionRule("org.slf4j"))
  }

  val kafkaStreams = "org.apache.kafka"           % "kafka-streams"   % KafkaVersion
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging"  % ScalaLoggingVersion
  val logback      = "ch.qos.logback"             % "logback-classic" % LogbackVersion
  val kafka        = "org.apache.kafka"           %% "kafka"          % KafkaVersion
  val curator      = "org.apache.curator"         % "curator-test"    % CuratorVersion
  val minitest     = "io.monix"                   %% "minitest"       % MinitestVersion
  val minitestLaws = "io.monix"                   %% "minitest-laws"  % MinitestVersion
  val algebird     = "com.twitter"                %% "algebird-core"  % AlgebirdVersion
  val chill        = "com.twitter"                %% "chill"          % ChillVersion
  val avro4s       = "com.sksamuel.avro4s"        %% "avro4s-core"    % Avro4sVersion
}
