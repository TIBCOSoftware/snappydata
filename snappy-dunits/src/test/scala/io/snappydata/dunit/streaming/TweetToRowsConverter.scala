package io.snappydata.dunit.streaming

import twitter4j.Status

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.StreamToRowsConverter
import org.apache.spark.unsafe.types.UTF8String

class TweetToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[Row] = {
    val status: Status = message.asInstanceOf[Status]
    Seq(Row.fromSeq(Seq(status.getId,
      UTF8String.fromString(status.getText),
      UTF8String.fromString(status.getUser().getName),
      UTF8String.fromString(status.getUser.getLang),
      status.getRetweetCount, UTF8String.fromString(
        status.getHashtagEntities.map(_.getText).mkString(",")))))
  }

}
