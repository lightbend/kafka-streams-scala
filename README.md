# A Thin Scala Wrapper Around the Kafka Streams Java API

[![Build Status](https://secure.travis-ci.org/lightbend/kafka-streams-scala.png)](http://travis-ci.org/lightbend/kafka-streams-scala)

The library wraps Java APIs in Scala thereby providing:

1. much better type inference in Scala
2. less boilerplate in application code
3. the usual builder-style composition that developers get with the original Java API

The design of the library was inspired by the work started by Alexis Seigneurin in [this repository](https://github.com/aseigneurin/kafka-streams-scala). 

## Quick Start

`kafka-streams-scala` is published and cross-built for Scala `2.11`, and `2.12`, so you can just add the following to your build:

```scala
val kafka_streams_scala_version = "0.1.2"

libraryDependencies ++= Seq("com.lightbend" %%
  "kafka-streams-scala" % kafka_streams_scala_version)
```

> Note: `kafka-streams-scala` supports onwards Kafka Streams `1.0.0`.

The API docs for `kafka-streams-scala` is available [here](https://developer.lightbend.com/docs/api/kafka-streams-scala/0.1.2/com/lightbend/kafka/scala/streams) for Scala 2.12 and [here](https://developer.lightbend.com/docs/api/kafka-streams-scala_2.11/0.1.2/#package) for Scala 2.11.

## Running the Tests

The library comes with an embedded Kafka server. To run the tests, simply run `sbt testOnly` and all tests will run on the local embedded server.

> The embedded server is started and stopped for every test and takes quite a bit of resources. Hence it's recommended that you allocate more heap space to `sbt` when running the tests. e.g. `sbt -mem 1500`.

```bash
$ sbt -mem 1500
> +clean
> +test
```

## Type Inference and Composition

Here's a sample code fragment using the Scala wrapper library. Compare this with the Scala code from the same [example](https://github.com/confluentinc/kafka-streams-examples/blob/4.0.0-post/src/test/scala/io/confluent/examples/streams/StreamToTableJoinScalaIntegrationTest.scala) in Confluent's repository.

```scala
// Compute the total per region by summing the individual click counts per region.
val clicksPerRegion: KTableS[String, Long] = userClicksStream

  // Join the stream against the table.
  .leftJoin(userRegionsTable, (clicks: Long, region: String) => (if (region == null) "UNKNOWN" else region, clicks))

  // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
  .map((_, regionWithClicks) => regionWithClicks)

  // Compute the total per region by summing the individual click counts per region.
  .groupByKey(Serialized.`with`(stringSerde, longSerde))
  .reduce(_ + _)
```

> **Notes:** 
> 
> 1. The left quotes around "with" are there because `with` is a Scala keyword. This is the mechanism you use to "escape" a Scala keyword when it's used as a normal identifier in a Java library.
> 2. Note that some methods, like `map`, take a two-argument function, for key-value pairs, rather than the more typical single argument.

## Better Abstraction

The wrapped Scala APIs also incur less boilerplate by taking advantage of Scala function literals that get converted to Java objects in the implementation of the API. Hence the surface syntax of the client API looks simpler and less noisy.

Here's an example of a snippet built using the Java API from Scala ..

```scala
val approximateWordCounts: KStream[String, Long] = textLines
  .flatMapValues(value => value.toLowerCase.split("\\W+").toIterable.asJava)
  .transform(
    new TransformerSupplier[Array[Byte], String, KeyValue[String, Long]] {
      override def get() = new ProbabilisticCounter
    },
    cmsStoreName)
approximateWordCounts.to(outputTopic, Produced.`with`(Serdes.String(), longSerde))
```

And here's the corresponding snippet using the Scala library. Note how the noise of `TransformerSupplier` has been abstracted out by the function literal syntax of Scala.

```scala
textLines
  .flatMapValues(value => value.toLowerCase.split("\\W+").toIterable)
  .transform(() => new ProbabilisticCounter, cmsStoreName)
  .to(outputTopic, Produced.`with`(Serdes.String(), longSerde))
```

Also, the explicit conversion `asJava` from a Scala `Iterable` to a Java `Iterable` is done for you by the Scala library.

## Implicit Serdes

One of the areas where the Java APIs' verbosity can be reduced is through a succinct way to pass serializers and de-serializers to the various functions. The library implementation offers implicit serdes to provide the serializers and de-serializers but at the same time also *allows the opt-in to use the default serializers registered in the Kafka Streams config*.

The optional implicit pattern is implemented with the usual null-default-value trick, but with a difference. The technique used is adopted from [this blog post](http://missingfaktor.blogspot.in/2013/12/optional-implicit-trick-in-scala.html).

The standard way to implement the null-default-value trick could not be applied as Scala [does not allow](https://stackoverflow.com/questions/4652095/why-does-the-scala-compiler-disallow-overloaded-methods-with-default-arguments/4652681#4652681) a mix of default values and function overloads. And we have quite a few examples of such overloaded functions in the Kafka Streams API set.

The implementation allows implicits for the `Serde`s or for `Serialized`, `Consumed` and `Produced`. The test examples demonstrate both, though the implicits for Serdes make a cleaner implementation.

The implementation does a trade-off in using the null-default-value trick as it moves some of the compile time errors to runtime.

### Examples

1. The example [StreamToTableJoinScalaIntegrationTestImplicitSerdes](https://github.com/lightbend/kafka-streams-scala/blob/develop/src/test/scala/com/lightbend/kafka/scala/streams/StreamToTableJoinScalaIntegrationTestImplicitSerdes.scala) demonstrates how to use the technique of implicit `Serde`s
2. The example [StreamToTableJoinScalaIntegrationTestImplicitSerialized](https://github.com/lightbend/kafka-streams-scala/blob/develop/src/test/scala/com/lightbend/kafka/scala/streams/StreamToTableJoinScalaIntegrationTestImplicitSerialized.scala) demonstrates how to use the technique of implicit `Serialized`, `Consumed` and `Produced`.
3. The example [StreamToTableJoinScalaIntegrationTestMixImplicitSerialized](https://github.com/lightbend/kafka-streams-scala/blob/develop/src/test/scala/com/lightbend/kafka/scala/streams/StreamToTableJoinScalaIntegrationTestMixImplicitSerialized.scala) demonstrates how to use the technique of how to use default serdes along with implicit `Serialized`, `Consumed` and `Produced`.
