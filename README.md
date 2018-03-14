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
val kafka_streams_scala_version = "0.2.0"

libraryDependencies ++= Seq("com.lightbend" %%
  "kafka-streams-scala" % kafka_streams_scala_version)
```

> Note: `kafka-streams-scala` supports onwards Kafka Streams `1.0.0`.

The API docs for `kafka-streams-scala` is available [here](https://developer.lightbend.com/docs/api/kafka-streams-scala/0.2.0/com/lightbend/kafka/scala/streams) for Scala 2.12 and [here](https://developer.lightbend.com/docs/api/kafka-streams-scala_2.11/0.2.0/#package) for Scala 2.11.

## Running the Tests

The library comes with an embedded Kafka server. To run the tests, simply run `sbt testOnly` and all tests will run on the local embedded server.

> The embedded server is started and stopped for every test and takes quite a bit of resources. Hence it's recommended that you allocate more heap space to `sbt` when running the tests. e.g. `sbt -mem 2000`.

```bash
$ sbt -mem 2000
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

One of the areas where the Java APIs' verbosity can be reduced is through a succinct way to pass serializers and de-serializers to the various functions. The library implementation offers type safe implicit serdes to provide the serializers and de-serializers. In doing so, the Scala library **does not use configuration based default serdes** which is not type safe and prone to runtime errors.

The implementation allows implicits for the `Serde`s or for `Serialized`, `Consumed`, `Produced` and `Joined`. The test examples demonstrate both, though the implicits for Serdes make a cleaner implementation.

### Default Serdes

The library offers a module that contains all the default serdes for the primitives. Importing the object will bring in scope all such primitives and helps reduce implicit hell.

```scala
object DefaultSerdes {
  implicit val stringSerde: Serde[String] = Serdes.String()
  implicit val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]
  implicit val byteArraySerde: Serde[Array[Byte]] = Serdes.ByteArray()
  implicit val bytesSerde: Serde[org.apache.kafka.common.utils.Bytes] = Serdes.Bytes()
  implicit val floatSerde: Serde[Float] = Serdes.Float().asInstanceOf[Serde[Float]]
  implicit val doubleSerde: Serde[Double] = Serdes.Double().asInstanceOf[Serde[Double]]
  implicit val integerSerde: Serde[Int] = Serdes.Integer().asInstanceOf[Serde[Int]]
}
```

### Compile time typesafe

Not only the serdes, but `DefaultSerdes` also brings into scope implicit  `Serialized`, `Produced`, `Consumed` and `Joined` instances. So all APIs that accept `Serialized`, `Produced`, `Consumed` or `Joined` will get these instances automatically with an `import DefaultSerdes._`.

Just one import of `DefaultSerdes._` and the following code does not need a bit of `Serialized`, `Produced`, `Consumed` or `Joined` to be specified explicitly or through the default config. **And the best part is that for any missing instances of these you get a compilation error.** ..

```scala
import DefaultSerdes._

val clicksPerRegion: KTableS[String, Long] =
  userClicksStream

  // Join the stream against the table.
  .leftJoin(userRegionsTable, (clicks: Long, region: String) => (if (region == null) "UNKNOWN" else region, clicks))

  // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
  .map((_, regionWithClicks) => regionWithClicks)

  // Compute the total per region by summing the individual click counts per region.
  .groupByKey
  .reduce(_ + _)

  // Write the (continuously updating) results to the output topic.
  clicksPerRegion.toStream.to(outputTopic)
```
