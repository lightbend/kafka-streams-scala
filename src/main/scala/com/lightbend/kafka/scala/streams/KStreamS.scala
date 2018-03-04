/**
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package com.lightbend.kafka.scala.streams

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}
import ImplicitConversions._
import FunctionConversions._

import scala.collection.JavaConverters._

/**
 * Wraps the Java class KStream and delegates method calls to the underlying Java object.
 */
class KStreamS[K, V](val inner: KStream[K, V]) {

  def filter(predicate: (K, V) => Boolean): KStreamS[K, V] = {
    inner.filter(predicate(_, _))
  }

  def filterNot(predicate: (K, V) => Boolean): KStreamS[K, V] = {
    inner.filterNot(predicate(_, _))
  }

  def selectKey[KR](mapper: (K, V) => KR): KStreamS[KR, V] = {
    inner.selectKey[KR]((k: K, v: V) => mapper(k, v))
  }

  def map[KR, VR](mapper: (K, V) => (KR, VR)): KStreamS[KR, VR] = {
    val kvMapper = mapper.tupled andThen tuple2ToKeyValue
    inner.map[KR, VR]((k, v) => kvMapper(k,v))
  }

  def mapValues[VR](mapper: V => VR): KStreamS[K, VR] = {
    inner.mapValues[VR](mapper(_))
  }

  def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)]): KStreamS[KR, VR] = {
    val kvMapper = mapper.tupled andThen (iter => iter.map(tuple2ToKeyValue).asJava)
    inner.flatMap[KR, VR]((k,v) => kvMapper(k , v))
  }

  def flatMapValues[VR](processor: V => Iterable[VR]): KStreamS[K, VR] = {
    inner.flatMapValues[VR]((v) => processor(v).asJava)
  }

  def print(printed: Printed[K, V]): Unit = inner.print(printed)

  def foreach(action: (K, V) => Unit): Unit = {
    inner.foreach((k, v) => action(k, v))
  }

  def branch(predicates: ((K, V) => Boolean)*): Array[KStreamS[K, V]] = {
    inner.branch(predicates.map(_.asPredicate): _*).map(kstream => wrapKStream(kstream))
  }

  def through(topic: String)(implicit produced: Perhaps[Produced[K, V]]): KStreamS[K, V] =
    produced.fold[KStreamS[K, V]] { inner.through(topic) } { ev => inner.through(topic, ev) }

  def to(topic: String)(implicit produced: Perhaps[Produced[K, V]]): Unit =
    produced.fold[Unit] { inner.to(topic) } { implicit ev => inner.to(topic, ev) }

  //scalastyle:off null
  def transform[K1, V1](transformerSupplier: () => Transformer[K, V, (K1, V1)],
    stateStoreNames: String*): KStreamS[K1, V1] = {

    val transformerSupplierJ: TransformerSupplier[K, V, KeyValue[K1, V1]] = () => {
      val transformerS: Transformer[K, V, (K1, V1)] = transformerSupplier()
      new Transformer[K, V, KeyValue[K1, V1]] {
        override def transform(key: K, value: V): KeyValue[K1, V1] = {
          transformerS.transform(key, value) match {
            case (k1,v1) => KeyValue.pair(k1, v1)
            case _ => null
          }
        }

        override def init(context: ProcessorContext): Unit = transformerS.init(context)

        @deprecated ("Please use Punctuator functional interface at https://kafka.apache.org/10/javadoc/org/apache/kafka/streams/processor/Punctuator.html instead", "0.1.3") // scalastyle:ignore
        override def punctuate(timestamp: Long): KeyValue[K1, V1] = {
          transformerS.punctuate(timestamp) match {
            case (k1, v1) => KeyValue.pair[K1, V1](k1, v1)
            case _ => null
          }
        }

        override def close(): Unit = transformerS.close()
      }
    }
    inner.transform(transformerSupplierJ, stateStoreNames: _*)
  }
  //scalastyle:on null

  def transformValues[VR](valueTransformerSupplier: () => ValueTransformer[V, VR],
    stateStoreNames: String*): KStreamS[K, VR] = {

    val valueTransformerSupplierJ: ValueTransformerSupplier[V, VR] = () => valueTransformerSupplier()
    inner.transformValues[VR](valueTransformerSupplierJ, stateStoreNames: _*)
  }

  def process(processorSupplier: () => Processor[K, V],
    stateStoreNames: String*): Unit = {

    val processorSupplierJ: ProcessorSupplier[K, V] = () => processorSupplier()
    inner.process(processorSupplierJ, stateStoreNames: _*)
  }

  /**
   * If `Serialized[K, V]` is found in the implicit scope, then use it, else
   * use the API with the default serializers.
   *
   * Usage Pattern 1: No implicits in scope, use default serializers
   * - .groupByKey
   *
   * Usage Pattern 2: Use implicit `Serialized` in scope
   * implicit val serialized = Serialized.`with`(stringSerde, longSerde)
   * - .groupByKey
   *
   * Usage Pattern 3: uses the implicit conversion from the serdes to `Serialized`
   * implicit val stringSerde: Serde[String] = Serdes.String()
   * implicit val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]
   * - .groupByKey
   */
  def groupByKey(implicit serialized: Perhaps[Serialized[K, V]]): KGroupedStreamS[K, V] =
    serialized.fold[KGroupedStreamS[K, V]] { inner.groupByKey } { implicit ev => inner.groupByKey(ev) }

  def groupBy[KR](selector: (K, V) => KR)(implicit serialized: Perhaps[Serialized[KR, V]]): KGroupedStreamS[KR, V] = {
    serialized.fold[KGroupedStreamS[KR, V]] {
      inner.groupBy(selector.asKeyValueMapper)
    } { implicit ev =>
      inner.groupBy(selector.asKeyValueMapper, ev)
    }
  }

  def join[VO, VR](otherStream: KStreamS[K, VO],
    joiner: (V, VO) => VR,
    windows: JoinWindows)(implicit joined: Perhaps[Joined[K, V, VO]]): KStreamS[K, VR] = {

    joined.fold[KStreamS[K, VR]] {
      inner.join[VO, VR](otherStream.inner, joiner.asValueJoiner, windows) } { implicit ev =>
      inner.join[VO, VR](otherStream.inner, joiner.asValueJoiner, windows, ev)
    }
  }

  def join[VT, VR](table: KTableS[K, VT],
    joiner: (V, VT) => VR)(implicit joined: Perhaps[Joined[K, V, VT]]): KStreamS[K, VR] = {

    joined.fold[KStreamS[K, VR]] {
      inner.leftJoin[VT, VR](table.inner, joiner.asValueJoiner) } { implicit ev =>
      inner.leftJoin[VT, VR](table.inner, joiner.asValueJoiner, ev)
    }
  }

  def join[GK, GV, RV](globalKTable: GlobalKTable[GK, GV],
    keyValueMapper: (K, V) => GK,
    joiner: (V, GV) => RV): KStreamS[K, RV] = {

    inner.join[GK, GV, RV](globalKTable, keyValueMapper(_,_), joiner(_,_))
  }

  def leftJoin[VO, VR](otherStream: KStreamS[K, VO],
    joiner: (V, VO) => VR,
    windows: JoinWindows)(implicit joined: Perhaps[Joined[K, V, VO]]): KStreamS[K, VR] = {

    joined.fold[KStreamS[K, VR]] {
      inner.leftJoin[VO, VR](otherStream.inner, joiner.asValueJoiner, windows) } { implicit ev =>
      inner.leftJoin[VO, VR](otherStream.inner, joiner.asValueJoiner, windows, ev)
    }
  }

  def leftJoin[VT, VR](table: KTableS[K, VT],
    joiner: (V, VT) => VR)(implicit joined: Perhaps[Joined[K, V, VT]]): KStreamS[K, VR] = {

    joined.fold[KStreamS[K, VR]] {
      inner.leftJoin[VT, VR](table.inner, joiner.asValueJoiner) } { implicit ev =>
      inner.leftJoin[VT, VR](table.inner, joiner.asValueJoiner, ev)
    }
  }

  def leftJoin[GK, GV, RV](globalKTable: GlobalKTable[GK, GV],
    keyValueMapper: (K, V) => GK,
    joiner: (V, GV) => RV): KStreamS[K, RV] = {

    inner.leftJoin[GK, GV, RV](globalKTable, keyValueMapper.asKeyValueMapper, joiner.asValueJoiner)
  }

  def outerJoin[VO, VR](otherStream: KStreamS[K, VO],
    joiner: (V, VO) => VR,
    windows: JoinWindows)(implicit joined: Perhaps[Joined[K, V, VO]]): KStreamS[K, VR] = {

    joined.fold[KStreamS[K, VR]] {
      inner.outerJoin[VO, VR](otherStream.inner, joiner.asValueJoiner, windows) } { implicit ev =>
      inner.outerJoin[VO, VR](otherStream.inner, joiner.asValueJoiner, windows, ev)
    }
  }

  def merge(stream: KStreamS[K, V]): KStreamS[K, V] = inner.merge(stream)

  def peek(action: (K, V) => Unit): KStreamS[K, V] = {
    inner.peek(action(_,_))
  }

  // -- EXTENSIONS TO KAFKA STREAMS --

  // applies the predicate to know what messages should go to the left stream (predicate == true)
  // or to the right stream (predicate == false)
  def split(predicate: (K, V) => Boolean): (KStreamS[K, V], KStreamS[K, V]) = {
    (this.filter(predicate), this.filterNot(predicate))
  }

}
