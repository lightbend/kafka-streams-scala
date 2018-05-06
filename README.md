**Note:** *Scala API for Kafka Streams have been accepted for inclusion in Apache Kafka. We have been working with the Kafka team since the last couple of months working towards meeting the standards and guidelines for this activity. Lightbend and Alexis Seigneurin have contributed this library (with some changes) to the Kafka community. This is already available on [Apache Kafka trunk](https://github.com/apache/kafka/tree/trunk/streams/streams-scala) and will be included in the upcoming release of Kafka. Hence it does not make much sense to update this project on a regular basis. For some time however, we will continue to provide support for fixing bugs only.*

# A Thin Scala Wrapper Around the Kafka Streams Java API

[![Build Status](https://secure.travis-ci.org/lightbend/kafka-streams-scala.png)](http://travis-ci.org/lightbend/kafka-streams-scala)

The library wraps Java APIs in Scala thereby providing:

1. much better type inference in Scala
2. less boilerplate in application code
3. the usual builder-style composition that developers get with the original Java API
4. complete compile time type safety

The design of the library was inspired by the work started by Alexis Seigneurin in [this repository](https://github.com/aseigneurin/kafka-streams-scala). 

## Quick Start

`kafka-streams-scala` is published and cross-built for Scala `2.11`, and `2.12`, so you can just add the following to your build:

```scala
val kafka_streams_scala_version = "0.2.1"

libraryDependencies ++= Seq("com.lightbend" %%
  "kafka-streams-scala" % kafka_streams_scala_version)
```

> Note: `kafka-streams-scala` supports onwards Kafka Streams `1.0.0`.

The API docs for `kafka-streams-scala` is available [here](https://developer.lightbend.com/docs/api/kafka-streams-scala/0.2.1/com/lightbend/kafka/scala/streams) for Scala 2.12 and [here](https://developer.lightbend.com/docs/api/kafka-streams-scala_2.11/0.2.1/#package) for Scala 2.11.

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
  .groupByKey
  .reduce(_ + _)
```

## Implicit Serdes

One of the areas where the Java APIs' verbosity can be reduced is through a succinct way to pass serializers and de-serializers to the various functions. The library uses the power of Scala implicits towards this end. The library makes some decisions that help implement more succinct serdes in a type safe manner:

1. No use of configuration based default serdes. Java APIs allow the user to define default key and value serdes as part of the configuration. This configuration, being implemented as `java.util.Properties` is type-unsafe and hence can result in runtime errors in case the user misses any of the serdes to be specified or plugs in an incorrect serde. `kafka-streams-scala` makes this completely type-safe by allowing all serdes to be specified through Scala implicits.
2. The library offers implicit conversions from serdes to `Serialized`, `Produced`, `Consumed` or `Joined`. Hence as a user you just have to pass in the implicit serde and all conversions to `Serialized`, `Produced`, `Consumed` or `Joined` will be taken care of automatically.


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
