package com.lightbend.kafka.scala.iq.example
package processor

import scala.util.{ Success, Failure }
import org.apache.kafka.streams.processor.{ AbstractProcessor, ProcessorContext, PunctuationType, Punctuator }
import models.{ LogParseUtil, LogRecord }
import com.typesafe.scalalogging.LazyLogging

class WeblogProcessor extends AbstractProcessor[String, String] with LazyLogging {
  private var bfStore: BFStore[String] = _ 

  override def init(context: ProcessorContext): Unit = {
    super.init(context)
    this.context.schedule(
      1000, 
      PunctuationType.WALL_CLOCK_TIME, 
      new Punctuator() { 
        override def punctuate(timestamp: Long): Unit = () 
      }
    )
    bfStore = this.context.getStateStore(WeblogDriver.LOG_COUNT_STATE_STORE).asInstanceOf[BFStore[String]]
  }

  override def process(dummy: String, record: String): Unit = LogParseUtil.parseLine(record) match {
    case Success(r) => { 
      bfStore + r.host
      bfStore.changeLogger.logChange(bfStore.changelogKey, bfStore.bf)
    }
    case Failure(ex) => {
      logger.warn(s"Error processing record $record .. skipping", ex)
    }
  }

  override def punctuate(timestamp: Long): Unit = super.punctuate(timestamp)
  override def close(): Unit = {}
}
