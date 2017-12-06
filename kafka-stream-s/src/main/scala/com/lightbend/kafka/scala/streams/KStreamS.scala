package com.lightbend.kafka.scala.streams

import ImplicitConversions._
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}

import scala.collection.JavaConverters._

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
    val kvMapper = mapper.tupled andThen Tuple2ToKeyValue
    inner.map[KR, VR]((k, v) => kvMapper(k,v))
  }

  def mapValues[VR](mapper: V => VR): KStreamS[K, VR] = {
    inner.mapValues[VR](mapper(_))
  }

  def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)]): KStreamS[KR, VR] = {
    val kvMapper = mapper.tupled andThen (iter => iter.map(Tuple2ToKeyValue).asJava)
    inner.flatMap[KR, VR]((k,v) => kvMapper(k , v))
  }

  def flatMapValues[VR](processor: V => Iterable[VR]): KStreamS[K, VR] = {
    inner.flatMapValues[VR]((v) => processor(v).asJava)
  }

  def print(printed: Printed[K, V]) = inner.print(printed)

  def foreach(action: (K, V) => Unit): Unit = {
    inner.foreach((k, v) => action(k, v))
  }

  def branch(predicates: ((K, V) => Boolean)*): Array[KStreamS[K, V]] = {
    inner.branch(predicates.map(_.asPredicate): _*).map(kstream => wrapKStream(kstream))
  }

  def through(topic: String): KStreamS[K, V] = inner.through(topic)

  def through(topic: String,
    produced: Produced[K, V]): KStreamS[K, V] = inner.through(topic, produced)

  def to(topic: String): Unit = inner.to(topic)

  def to(topic: String,
    produced: Produced[K, V]): Unit = inner.to(topic, produced)

  def transform[K1, V1](transformerSupplier: () => Transformer[K, V, (K1, V1)],
    stateStoreNames: String*): KStreamS[K1, V1] = {

    val transformerSupplierJ: TransformerSupplier[K, V, KeyValue[K1, V1]] = () => {
      val transformerS: Transformer[K, V, (K1, V1)] = transformerSupplier()
      new Transformer[K, V, KeyValue[K1, V1]] {
        override def transform(key: K, value: V): KeyValue[K1, V1] = {
          val (k1,v1) = transformerS.transform(key, value)
          new KeyValue[K1, V1](k1, v1)
        }

        override def init(context: ProcessorContext): Unit = transformerS.init(context)

        override def punctuate(timestamp: Long): KeyValue[K1, V1] = {
          val (k1,v1) = transformerS.punctuate(timestamp)
          new KeyValue[K1, V1](k1, v1)
        }

        override def close(): Unit = transformerS.close()
      }
    }
    inner.transform(transformerSupplierJ, stateStoreNames: _*)
  }

  def transformValues[VR](valueTransformerSupplier: () => ValueTransformer[V, VR],
    stateStoreNames: String*): KStreamS[K, VR] = {

    val valueTransformerSupplierJ: ValueTransformerSupplier[V, VR] = () => valueTransformerSupplier()
    inner.transformValues[VR](valueTransformerSupplierJ, stateStoreNames: _*)
  }

  def process(processorSupplier: () => Processor[K, V],
    stateStoreNames: String*) = {

    val processorSupplierJ: ProcessorSupplier[K, V] = () => processorSupplier()
    inner.process(processorSupplierJ, stateStoreNames: _*)
  }

  def groupByKey(): KGroupedStreamS[K, V] =
    inner.groupByKey()

  def groupByKey(serialized: Serialized[K, V]): KGroupedStreamS[K, V] =
    inner.groupByKey(serialized)

  def groupBy[KR](selector: (K, V) => KR): KGroupedStreamS[KR, V] = {
    inner.groupBy(selector(_, _))
  }

  def groupBy[KR](selector: (K, V) => KR, serialized: Serialized[KR, V]): KGroupedStreamS[KR, V] = {
    val selectorJ: KeyValueMapper[K, V, KR] = (k, v) => selector(k, v)
    inner.groupBy(selector(_, _), serialized)
  }

  def join[VO, VR](otherStream: KStreamS[K, VO],
    joiner: (V, VO) => VR,
    windows: JoinWindows): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[V, VO, VR] = (v1, v2) => joiner(v1, v2)
    inner.join[VO, VR](otherStream.inner, joinerJ, windows)
  }

  def join[VO, VR](otherStream: KStreamS[K, VO],
    joiner: (V, VO) => VR,
    windows: JoinWindows,
    joined: Joined[K, V, VO]): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[V, VO, VR] = (v1, v2) => joiner(v1, v2)
    inner.join[VO, VR](otherStream.inner, joinerJ, windows, joined)
  }

  def join[VT, VR](table: KTableS[K, VT],
    joiner: (V, VT) => VR): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[V, VT, VR] = (v1, v2) => joiner(v1, v2)
    inner.join[VT, VR](table.inner, joinerJ)
  }

  def join[VT, VR](table: KTableS[K, VT],
    joiner: (V, VT) => VR,
    joined: Joined[K, V, VT]): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[V, VT, VR] = (v1, v2) => joiner(v1, v2)
    inner.join[VT, VR](table.inner, joinerJ, joined)
  }

  def join[GK, GV, RV](globalKTable: GlobalKTable[GK, GV],
    keyValueMapper: (K, V) => GK,
    joiner: (V, GV) => RV): KStreamS[K, RV] = {

    inner.join[GK, GV, RV](globalKTable, keyValueMapper(_,_), joiner(_,_))
  }

  def leftJoin[VO, VR](otherStream: KStreamS[K, VO],
    joiner: (V, VO) => VR,
    windows: JoinWindows): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[V, VO, VR] = (v1, v2) => joiner(v1, v2)
    inner.leftJoin[VO, VR](otherStream.inner, joinerJ, windows)
  }

  def leftJoin[VO, VR](otherStream: KStreamS[K, VO],
    joiner: (V, VO) => VR,
    windows: JoinWindows,
    joined: Joined[K, V, VO]): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[V, VO, VR] = (v1, v2) => joiner(v1, v2)
    inner.leftJoin[VO, VR](otherStream.inner, joinerJ, windows, joined)
  }

  def leftJoin[VT, VR](table: KTableS[K, VT],
    joiner: (V, VT) => VR): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[V, VT, VR] = (v1, v2) => joiner(v1, v2)
    inner.leftJoin[VT, VR](table.inner, joinerJ)
  }

  def leftJoin[VT, VR](table: KTableS[K, VT],
    joiner: (V, VT) => VR,
    joined: Joined[K, V, VT]): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[V, VT, VR] = (v1, v2) => joiner(v1, v2)
    inner.leftJoin[VT, VR](table.inner, joinerJ, joined)
  }

  def leftJoin[GK, GV, RV](globalKTable: GlobalKTable[GK, GV],
    keyValueMapper: (K, V) => GK,
    joiner: (V, GV) => RV): KStreamS[K, RV] = {

    val joinerJ: ValueJoiner[V, GV, RV] = (v1, v2) => joiner(v1, v2)
      val keyValueMapperJ: KeyValueMapper[K, V, GK] = (k, v) => keyValueMapper(k, v)
    inner.leftJoin[GK, GV, RV](globalKTable, keyValueMapperJ, joinerJ)
  }

  def outerJoin[VO, VR](otherStream: KStreamS[K, VO],
    joiner: (V, VO) => VR,
    windows: JoinWindows): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[V, VO, VR] = (v1, v2) => joiner(v1, v2)
    inner.outerJoin[VO, VR](otherStream.inner, joinerJ, windows)
  }

  def outerJoin[VO, VR](otherStream: KStreamS[K, VO],
    joiner: (V, VO) => VR,
    windows: JoinWindows,
    joined: Joined[K, V, VO]): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[V, VO, VR] = (v1, v2) => joiner(v1, v2)
    inner.outerJoin[VO, VR](otherStream.inner, joinerJ, windows, joined)
  }

  def merge(stream: KStreamS[K, V]): KStreamS[K, V] = inner.merge(stream)

  def peek(action: (K, V) => Unit): KStream[K, V] = {
    inner.peek(action(_,_))
  }

  // -- EXTENSIONS TO KAFKA STREAMS --

  // applies the predicate to know what messages shuold go to the left stream (predicate == true)
  // or to the right stream (predicate == false)
  def split(predicate: (K, V) => Boolean): (KStreamS[K, V], KStreamS[K, V]) = {
    (this.filter(predicate), this.filterNot(predicate))
  }

}
