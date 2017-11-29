name := "kafka-streams-scala"

organization := "com.lightbend"

version := "0.0.1"

val algebirdVersion = "0.13.0"
val chillVersion = "0.9.2"
val logbackVersion = "1.2.3"
val kafkaVersion = "1.0.0"
val scalaLoggingVersion = "3.5.0"
val curatorVersion = "4.0.0"
val minitestVersion = "2.0.0"

scalaVersion := "2.12.4"

crossScalaVersions := Seq("2.12.4", "2.11.11")

scalacOptions := Seq("-Xexperimental", "-unchecked", "-deprecation")

parallelExecution in Test := false

val kafkaStreams = "org.apache.kafka" % "kafka-streams" % kafkaVersion
val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion
val logback = "ch.qos.logback" % "logback-classic" % logbackVersion

val kafka = "org.apache.kafka" % "kafka_2.12" % kafkaVersion
val curator = "org.apache.curator" % "curator-test" % curatorVersion
val minitest = "io.monix" %% "minitest" % minitestVersion
val minitestLaws = "io.monix" %% "minitest-laws" % minitestVersion

val algebird = "com.twitter" %% "algebird-core" % algebirdVersion 
val chill = "com.twitter" %% "chill" % chillVersion 

libraryDependencies ++= Seq(
  kafkaStreams excludeAll(ExclusionRule("org.slf4j", "slf4j-log4j12"), ExclusionRule("org.apache.zookeeper", "zookeeper")),
  scalaLogging % "test",
  logback % "test",
  kafka % "test" excludeAll(ExclusionRule("org.slf4j", "slf4j-log4j12"), ExclusionRule("org.apache.zookeeper", "zookeeper")),
  curator % "test",
  minitest % "test",
  minitestLaws % "test",
  algebird % "test",
  chill % "test"
)

testFrameworks += new TestFramework("minitest.runner.Framework")

publishTo := Some(Resolver.file("file", new File(Path.userHome.absolutePath + "/.m2/repository")))
