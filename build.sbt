import Dependencies._

name := "kafka-streams-scala"

organization := "com.lightbend"

version := "0.1.2"

scalaVersion := Versions.Scala_2_12_Version

crossScalaVersions := Versions.CrossScalaVersions

scalacOptions := Seq("-Xexperimental", "-unchecked", "-deprecation", "-Ywarn-unused-import")

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

licenses := Seq("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))

developers := List(
  Developer("debasishg", "Debasish Ghosh", "@debasishg", url("https://github.com/debasishg")),
  Developer("blublinsky", "Boris Lublinsky", "@blublinsky", url("https://github.com/blublinsky")),
  Developer("maasg", "Gerard Maas", "@maasg", url("https://github.com/maasg")),
  Developer("ssaavedra", "Santiago Saavedra", "@ssaavedra", url("https://github.com/ssaavedra"))
)

organizationName := "lightbend"

organizationHomepage := Some(url("http://lightbend.com/"))

homepage := scmInfo.value map (_.browseUrl)

scmInfo := Some(ScmInfo(url("https://github.com/lightbend/kafka-streams-scala"), "git@github.com:lightbend/kafka-streams-scala.git"))

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true

publishArtifact in Test := false
