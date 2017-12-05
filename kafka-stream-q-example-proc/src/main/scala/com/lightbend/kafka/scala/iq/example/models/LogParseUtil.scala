package com.lightbend.kafka.scala.iq.example
package models

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import scala.util.Try

object LogParseUtil {
  final val logRegex = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)""".r

  def parseLine(line: String): Try[LogRecord] = Try {
    logRegex.findFirstIn(line) match {
      case Some(logRegex(host, clientId, user, timestamp, method, endpoint, protocol, httpReplyCode, bytes)) =>
        LogRecord(host, clientId, user, parseTimestamp(timestamp), method, endpoint, protocol, httpReplyCode.toInt, toSafeInt(bytes))
      case _ => throw new IllegalArgumentException(s"Cannot parse line $line")
    }
  }

  private def parseTimestamp(s: String): OffsetDateTime = {
    val f = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z")
    OffsetDateTime.from(f.parse(s))
  }

  private def toSafeInt(s: String): Int = try {
    s.toInt
  } catch {
    case _: Exception => 0
  }
}
