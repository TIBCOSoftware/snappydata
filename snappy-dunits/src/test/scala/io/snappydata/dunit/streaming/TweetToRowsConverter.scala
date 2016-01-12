package io.snappydata.dunit.streaming

import twitter4j.Status

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.streaming.StreamToRowsConverter
import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by ymahajan on 30/12/15.
  */
class TweetToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[InternalRow] = {
    val status: Status = message.asInstanceOf[Status]
    Seq(InternalRow.fromSeq(Seq(status.getId,
      UTF8String.fromString(status.getText),
      UTF8String.fromString(status.getUser().getName),
      UTF8String.fromString(status.getUser.getLang),
      status.getRetweetCount, UTF8String.fromString(
        status.getHashtagEntities.mkString(",")))))
  }

}
