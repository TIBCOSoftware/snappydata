package io.snappydata.app.twitter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.streaming.MessageToRowConverter
import org.apache.spark.unsafe.types.UTF8String
import twitter4j.{TwitterObjectFactory, Status}

/**
 * Created by ymahajan on 3/12/15.
 */
class LineToRowConverter extends MessageToRowConverter with Serializable {

  override def toRow(message: Any): InternalRow = {
    //val status: Status = TwitterObjectFactory.createStatus(message.asInstanceOf[String])
    InternalRow.fromSeq(Seq(message.toString))
  }
  override def getTargetType = classOf[String]
}
