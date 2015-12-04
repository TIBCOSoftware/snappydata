package io.snappydata.app.twitter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.streaming.MessageToRowConverter
import org.apache.spark.unsafe.types.UTF8String

/**
 * Created by ymahajan on 4/12/15.
 */

class SocketToRowConverter extends MessageToRowConverter with Serializable {

  override def toRow(message: Any): InternalRow = {
//    if(message.toString.equals("()")){
//      InternalRow.fromSeq(Seq(UTF8String.fromString("yogs"), 0))
//    }else {
//      val str = message.toString.split(',')
//      InternalRow.fromSeq(Seq(UTF8String.fromString(str(0)), str(1).toInt))
//    }
    InternalRow.fromSeq(Seq(UTF8String.fromString(message.toString)))
  }

  override def getTargetType = classOf[String]
}