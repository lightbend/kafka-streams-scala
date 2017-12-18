# Kafka Streams Goodies for Scala developers

This repository contains the following:

1. [Scala APIs for Kafka Streams](https://github.com/lightbend/kafka-streams-scala/blob/develop/kafka-stream-s/README.md): This is a thin wrapper on top of Java APIs to provide less boilerplates and better type inference.
2. [An http layer for Kafka Streams Interactive Queries](https://github.com/lightbend/kafka-streams-scala/blob/develop/kafka-stream-q/README.md): This is a utility that's quite useful for developing global queries across local states in a Kafka Streams application. More useful when the application is deployed in a distributed manner across multiple nodes.
3. [An example application](https://github.com/lightbend/kafka-streams-scala/blob/develop/kafka-stream-q-example-dsl/README.md) based on Kafka Streams DSL that uses the library in (2).

These tools support Kafka 1.0.0. By default, they build for Scala 2.12 (with Scala 2.12.4), but you can build targets for both 2.12 and 2.11 (using Scala 2.11.11) in SBT by adding a plus, `+`, before each command.

We have also found that you need more than the default memory for SBT, at least for the `kafka-stream-s` subproject (such as when running the tests). In the following example, we specify 1500MB:

```
$ sbt -mem 1500
> +clean
> +test    // for kafka-stream-s only
> +publishLocal
```

You can use this SBT invocation command for all four subprojects.
