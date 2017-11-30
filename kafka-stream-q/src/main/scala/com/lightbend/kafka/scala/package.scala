package com.lightbend.kafka.scala

import java.nio.charset.Charset

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import akka.pattern.after
import akka.actor.Scheduler

package object iq {
  final val CHARSET = Charset.forName("UTF-8")

  def translateHostInterface(host: String) = host match {
    case "0.0.0.0" => java.net.InetAddress.getLocalHost.getHostAddress
    case x => x
  }

  /**
   * Given an operation that produces a T, returns a Future containing the result of T, unless an exception is thrown,
   * in which case the operation will be retried after _delay_ time, if there are more possible retries, which is configured through
   * the _retries_ parameter. If the operation does not succeed and there is no retries left, the resulting Future will 
   * contain the last failure.
   **/
  // https://gist.github.com/viktorklang/9414163
  def retry[T](op: => T, delay: FiniteDuration, retries: Int)(implicit ec: ExecutionContext, s: Scheduler): Future[T] =
    Future(op) recoverWith { case _ if retries > 0 => after(delay, s)(retry(op, delay, retries - 1)) }
}
