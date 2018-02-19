/*
 * Copyright 2018 OpenShine SL <https://www.openshine.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lightbend.kafka.scala.streams.typesafe

import scala.language.higherKinds

/** Unsafe operations enabler.
  *
  * When introducing this object into scope you will gain access to
  * [[com.lightbend.kafka.scala.streams.typesafe.unsafe.UnsafelyWrapper]] which
  * enables the `unsafely` definition. You may use that to tap into the
  * underlying `unsafe` value and always retrieve a wrapped safe value back.
  *
  * In order not to break encapsulation, if you need to output values that
  * are outside the provided library, you will have to provide a
  * [[com.lightbend.kafka.scala.streams.typesafe!.ConverterToTypeSafer]]
  * instance providing a conversion from Src to the safe Dst.
  */
object unsafe {

  trait ConverterToTypeSafer[-Src[K, V], +Dst[K, V]] {
    def safe[K, V](src: Src[K, V]): Dst[K, V]
  }

  /** This class provides `.unsafely` instances for
    * [[com.lightbend.kafka.scala.streams.typesafe.TSKType]] values that may be
    * needed to provide values that cannot be assumed to be type-safe. You
    * can also use the `.unsafelyNoWrap` construct which does not wrap the
    * returned type (and thus you will have an abstraction leak, but you
    * needn't provide the type conversion).
    *
    * @param inner the TSKType that is being unsafely handled
    * @tparam KType the underlying implementation type (A KStream/KTable/etc)
    * @tparam K     the type of the key for this TSKType
    * @tparam V     the type of values for this TSKType
    */
  implicit class UnsafelyWrapper[KType[_, _], K, V]
  (private val inner: TSKType[KType, K, V])
  extends AnyVal {

    /** Allows the user to perform potentially-unsafe operations with the
      * TSKType, which may be usesful for accessing properties not yet
      * implemented on this library.
      *
      * Take note that in order not to leak the type-unsound abstraction you
      * must provide a [[ConverterToTypeSafer]] which should wrap the
      * value of `transformer` into its returned type. Your provided function
      * should operate only on the underlying types.
      *
      * Take note that it is assumed as of now that your operation will
      * return a (*, *)-kind type, with "holes" for a resulting key and value.
      * If you can't operate under such constraint, you will need to rely on
      * [[unsafelyNoWrap()]] for the moment.
      *
      * @param transformer the function that transforms the inner type
      * @param wrap        the evidence used to wrap the transformer output
      *                    back to a typesafe value (needn't be a TSKType, so
      *                    you can implement your own conversions)
      * @tparam K1        the resulting key type
      * @tparam V1        the resulting value type
      * @tparam UnsafeDst the type returned by the transformer, which is a
      *                   raw type in the underlying implementation
      * @tparam SafeDst   the type that should be returned after performing the
      *                   unsafe operation and which should wrap the type
      *                   returned by the transformer in a more
      *                   type-constrained way
      * @return a SafeDst value that has been transformed by the transformer
      *         on its underlying values
      */
    def unsafely[K1, V1, UnsafeDst[_, _], SafeDst[_, _]]
    (transformer: KType[K, V] => UnsafeDst[K1, V1])
    (implicit wrap: ConverterToTypeSafer[UnsafeDst, SafeDst])
    : SafeDst[K1, V1] = wrap.safe(transformer(inner.unsafe))

    /** Allows the user to perform potentially-unsafe operations with the
      * TSKType. Take note that this abstraction leaks Dst to the callee.
      *
      * @param transformer the transformation to apply to the underlying type
      * @tparam Dst the type of the result of the transformation
      * @return the result of the transformation
      */
    def unsafelyNoWrap[Dst](transformer: KType[K, V] => Dst) =
      transformer(inner.unsafe)
  }

}
