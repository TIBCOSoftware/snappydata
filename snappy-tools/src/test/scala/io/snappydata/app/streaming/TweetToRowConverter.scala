package io.snappydata.app.streaming

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.streaming.MessageToRowConverter
import org.apache.spark.unsafe.types.UTF8String
import twitter4j.Status

/**
 * Created by ymahajan on 8/12/15.
 */
class TweetToRowConverter extends MessageToRowConverter with Serializable {

  override def toRow(message: Any): InternalRow = {
    val status: Status = message.asInstanceOf[Status]
    InternalRow.fromSeq(Seq(status.getId, UTF8String.fromString(status.getText),
      UTF8String.fromString(status.getUser().getName), UTF8String.fromString(status.getUser.getLang),
      status.getRetweetCount, UTF8String.fromString(status.getHashtagEntities.mkString(","))))
  }

  override def getTargetType = classOf[String]
}
