name := "kafka-streams-scala"

organization := "com.lightbend"

version := "0.0.1"

scalaVersion := "2.12.4"

crossScalaVersions := Seq("2.12.4", "2.11.11")

scalacOptions := Seq("-Xexperimental", "-unchecked", "-deprecation")

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "1.0.0"
libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "1.0.0"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "org.apache.curator" % "curator-test" % "4.0.0"
libraryDependencies += "com.typesafe.akka" % "akka-http_2.11" % "10.0.10"
libraryDependencies += "de.heikoseeberger" % "akka-http-jackson_2.11" % "1.18.1"


publishTo := Some(Resolver.file("file", new File(Path.userHome.absolutePath + "/.m2/repository")))
