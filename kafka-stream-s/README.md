# A Thin Scala Wrapper Around the Kafka Streams Java API

The library wraps Java APIs in Scala thereby providing:

1. much better type inference in Scala
2. less boilerplate in application code
3. the usual builder-style composition that developers get with the original Java API

The design of the library was inspired by the work started by Alexis Seigneurin in [this repository](https://github.com/aseigneurin/kafka-streams-scala). 

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

## Build the Library

```
$ sbt -mem 1500
> +clean
> +test
> +publishLocal
```

