package com.lightbend.kafka.scala.iq.example
package models

import java.time.OffsetDateTime

case class LogRecord(
  host: String, 
  clientId: String, 
  user: String, 
  timestamp: OffsetDateTime, 
  method: String,
  endpoint: String, 
  protocol: String, 
  httpReplyCode: Int, 
  payloadSize: Long
)
