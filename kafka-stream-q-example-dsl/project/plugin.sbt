resolvers += "Bintray Repository" at "https://dl.bintray.com/shmishleniy/"

resolvers += "JAnalyse Repository" at "http://www.janalyse.fr/repository/"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.2")

addSbtPlugin("com.cavorite" % "sbt-avro-1-8" % "1.1.3")
