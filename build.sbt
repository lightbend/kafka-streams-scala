name := "kafka-streams-scala"

organization := "com.lightbend"

version := "0.0.1"

scalaVersion := "2.12.4"

crossScalaVersions := Seq("2.12.4", "2.11.11")

scalacOptions := Seq("-Xexperimental", "-unchecked", "-deprecation")

parallelExecution in Test := false

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "1.0.0" exclude("org.apache.zookeeper", "zookeeper")
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"

libraryDependencies += "org.apache.kafka" % "kafka_2.12" % "1.0.0" % "test" exclude("org.apache.zookeeper", "zookeeper")
libraryDependencies += "org.apache.curator" % "curator-test" % "4.0.0" % "test"
libraryDependencies += "io.monix" %% "minitest" % "2.0.0" % "test"
libraryDependencies += "io.monix" %% "minitest-laws" % "2.0.0" % "test"

libraryDependencies += "io.circe" %% "circe-core"    % "0.8.0" % "test"
libraryDependencies += "io.circe" %% "circe-generic" % "0.8.0" % "test"
libraryDependencies += "io.circe" %% "circe-parser"  % "0.8.0" % "test"

testFrameworks += new TestFramework("minitest.runner.Framework")

publishTo := Some(Resolver.file("file", new File(Path.userHome.absolutePath + "/.m2/repository")))
