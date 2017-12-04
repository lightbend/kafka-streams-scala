# Kafka Streams goodies for Scala developers

This repository contains the following:

1. [Scala APIs for Kafka Streams](https://github.com/lightbend/kafka-streams-scala/blob/develop/kafka-stream-s/README.md): This is a thin wrapper on top of Java APIs to provide less boilerplates and better type inference. 
2. An http layer for Kafka Straems Interactive Queries: This is a utility that's quite useful for developing global queries across local states in a Kafka Streams application. More useful when the application is deployed in a distributed manner across multiple nodes.
3. An example application based on Kafka Straems DSL that uses the library in (2).