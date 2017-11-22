package com.lightbend.kafka.scala.streams

import java.lang

import ImplicitConversions._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier, StreamPartitioner}

import scala.collection.JavaConverters._

class KStreamS[K, V](val inner: KStream[K, V]) {

  def filter(predicate: (K, V) => Boolean): KStreamS[K, V] = {
    val predicateJ: Predicate[K, V] = (k, v) => predicate(k, v)
    inner.filter(predicateJ)
  }

  def filterNot(predicate: (K, V) => Boolean): KStreamS[K, V] = {
    val predicateJ: Predicate[K, V] = (k, v) => predicate(k, v)
    inner.filterNot(predicateJ)
  }

  def selectKey[KR](mapper: (K, V) => KR): KStreamS[KR, V] = {
    val mapperJ: KeyValueMapper[K, V, KR] = (k: K, v: V) => mapper(k, v)
    inner.selectKey[KR](mapperJ)
  }

  def map[KR, VR](mapper: (K, V) => (KR, VR)): KStreamS[KR, VR] = {
    val mapperJ: KeyValueMapper[K, V, KeyValue[KR, VR]] = (k: K, v: V) => {
      val res = mapper(k, v)
      new KeyValue[KR, VR](res._1, res._2)
    }
    inner.map[KR, VR](mapperJ)
  }

  def mapValues[VR](mapper: (V => VR)): KStreamS[K, VR] = {
    val mapperJ: ValueMapper[V, VR] = (v: V) => mapper(v)
    inner.mapValues[VR](mapperJ)
  }

  def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)]): KStreamS[KR, VR] = {
    val mapperJ: KeyValueMapper[K, V, java.lang.Iterable[KeyValue[KR, VR]]] = (k: K, v: V) => {
      val resTuples: Iterable[(KR, VR)] = mapper(k, v)
      val res: Iterable[KeyValue[KR, VR]] = resTuples.map(t => new KeyValue[KR, VR](t._1, t._2))
      asJavaIterable(res)
    }
    inner.flatMap[KR, VR](mapperJ)
  }

  def flatMapValues[VR](processor: V => Iterable[VR]): KStreamS[K, VR] = {
    val processorJ: ValueMapper[V, java.lang.Iterable[VR]] = (v: V) => {
      val res: Iterable[VR] = processor(v)
      asJavaIterable(res)
    }
    inner.flatMapValues[VR](processorJ)
  }

  def print(printed: Printed[K, V]) = inner.print(printed)

  def foreach(action: (K, V) => Unit): Unit = {
    val actionJ: ForeachAction[_ >: K, _ >: V] = (k: K, v: V) => action(k, v)
    inner.foreach(actionJ)
  }

  def branch(predicates: ((K, V) => Boolean)*): Array[KStreamS[K, V]] = {
    val predicatesJ = predicates.map(predicate => {
      val predicateJ: Predicate[K, V] = (k, v) => predicate(k, v)
      predicateJ
    })
    inner.branch(predicatesJ: _*)
      .map(kstream => wrapKStream(kstream))
  }

  def through(topic: String): KStreamS[K, V] = inner.through(topic)

  def through(topic: String,
    produced: Produced[K, V]): KStreamS[K, V] = inner.through(topic, produced)

  def to(topic: String): KStreamS[K, V] = inner.through(topic)

  def to(topic: String,
    produced: Produced[K, V]): KStreamS[K, V] = inner.through(topic, produced)

  def transform[K1, V1](transformerSupplier: () => Transformer[K, V, (K1, V1)],
                        stateStoreNames: String*): KStreamS[K1, V1] = {

    val transformerSupplierJ: TransformerSupplier[K, V, KeyValue[K1, V1]] = () => {
      val transformerS: Transformer[K, V, (K1, V1)] = transformerSupplier()
      new Transformer[K, V, KeyValue[K1, V1]] {
        override def transform(key: K, value: V): KeyValue[K1, V1] = {
          val res = transformerS.transform(key, value)
          new KeyValue[K1, V1](res._1, res._2)
        }

        override def init(context: ProcessorContext): Unit = transformerS.init(context)

        override def punctuate(timestamp: Long): KeyValue[K1, V1] = {
          val res = transformerS.punctuate(timestamp)
          new KeyValue[K1, V1](res._1, res._2)
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

  def groupBy[KR, SK >: K, SV >: V](selector: (SK, SV) => KR): KGroupedStreamS[KR, V] = {
    val selectorJ: KeyValueMapper[SK, SV, KR] = (k: SK, v: SV) => selector(k, v)
    inner.groupBy(selectorJ)
  }

  def groupBy[KR, SK >: K, SV >: V](selector: (SK, SV) => KR, serialized: Serialized[KR, V]): KGroupedStreamS[KR, V] = {
    val selectorJ: KeyValueMapper[SK, SV, KR] = (k: SK, v: SV) => selector(k, v)
    inner.groupBy(selectorJ, serialized)
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

  def join[VT, VR, SV >: V, SVT >: VT, EVR <: VR](table: KTableS[K, VT],
    joiner: (SV, SVT) => EVR): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[SV, SVT, EVR] = (v1, v2) => joiner(v1, v2)
    inner.join[VT, VR](table.inner, joinerJ)
  }

  def join[VT, VR, SV >: V, SVT >: VT, EVR <: VR](table: KTableS[K, VT],
    joiner: (SV, SVT) => EVR,
    joined: Joined[K, V, VT]): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[SV, SVT, EVR] = (v1, v2) => joiner(v1, v2)
    inner.join[VT, VR](table.inner, joinerJ, joined)
  }

  def join[GK, GV, RV, SK >: K, SV >: V, EGK <: GK, SGV >: GV, ERV <: RV](globalKTable: GlobalKTable[GK, GV],
    keyValueMapper: (SK, SV) => EGK,
    joiner: (SV, SGV) => ERV): KStreamS[K, RV] = {

    val joinerJ: ValueJoiner[SV, SGV, ERV] = (v1, v2) => joiner(v1, v2)
    val keyValueMapperJ: KeyValueMapper[SK, SV, EGK] = (k, v) => keyValueMapper(k, v)
    inner.join[GK, GV, RV](globalKTable, keyValueMapperJ, joinerJ)
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

  def leftJoin[VT, VR, SV >: V, SVT >: VT, EVR <: VR](table: KTableS[K, VT],
    joiner: (SV, SVT) => EVR): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[SV, SVT, EVR] = (v1, v2) => joiner(v1, v2)
    inner.leftJoin[VT, VR](table.inner, joinerJ)
  }

  def leftJoin[VT, VR, SV >: V, SVT >: VT, EVR <: VR](table: KTableS[K, VT],
    joiner: (SV, SVT) => EVR,
    joined: Joined[K, V, VT]): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[SV, SVT, EVR] = (v1, v2) => joiner(v1, v2)
    inner.leftJoin[VT, VR](table.inner, joinerJ, joined)
  }

  def leftJoin[GK, GV, RV, SK >: K, SV >: V, EGK <: GK, SGV >: GV, ERV <: RV](globalKTable: GlobalKTable[GK, GV],
    keyValueMapper: (SK, SV) => EGK,
    joiner: (SV, SGV) => ERV): KStreamS[K, RV] = {

    val joinerJ: ValueJoiner[SV, SGV, ERV] = (v1, v2) => joiner(v1, v2)
    val keyValueMapperJ: KeyValueMapper[SK, SV, EGK] = (k, v) => keyValueMapper(k, v)
    inner.leftJoin[GK, GV, RV](globalKTable, keyValueMapperJ, joinerJ)
  }

  def outerJoin[VO, VR](otherStream: KStreamS[K, VO],
    joiner: (_ >: V, _ >: VO) => _ <: VR,
    windows: JoinWindows): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[V, VO, VR] = (v1, v2) => joiner(v1, v2)
    inner.outerJoin[VO, VR](otherStream.inner, joinerJ, windows)
  }

  def outerJoin[VO, VR](otherStream: KStreamS[K, VO],
    joiner: (_ >: V, _ >: VO) => _ <: VR,
    windows: JoinWindows,
    joined: Joined[K, V, VO]): KStreamS[K, VR] = {

    val joinerJ: ValueJoiner[V, VO, VR] = (v1, v2) => joiner(v1, v2)
    inner.outerJoin[VO, VR](otherStream.inner, joinerJ, windows, joined)
  }

  // -- EXTENSIONS TO KAFKA STREAMS --

  // applies the predicate to know what messages shuold go to the left stream (predicate == true)
  // or to the right stream (predicate == false)
  def split(predicate: (K, V) => Boolean): (KStreamS[K, V], KStreamS[K, V]) = {
    (this.filter(predicate), this.filterNot(predicate))
  }

}
