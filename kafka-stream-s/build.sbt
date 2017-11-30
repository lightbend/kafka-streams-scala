import Dependencies._

name := "kafka-streams-scala"

organization := "com.lightbend"

version := "0.0.1"

scalaVersion := Versions.scalaVersion

crossScalaVersions := Versions.crossScalaVersions

scalacOptions := Seq("-Xexperimental", "-unchecked", "-deprecation")

parallelExecution in Test := false

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
