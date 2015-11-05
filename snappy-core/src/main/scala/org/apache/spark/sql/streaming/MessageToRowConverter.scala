package org.apache.spark.sql.streaming

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{RowFactory, Row}
import org.apache.spark.unsafe.types.UTF8String

import twitter4j.Status
/**
 * Created by ymahajan on 4/11/15.
 */
trait MessageToRowConverter extends Serializable {
  def toRow(message: String): InternalRow
}

class MyMessageToRowConverter extends MessageToRowConverter with Serializable {
  override def toRow(message: String): InternalRow = {
    //TODO Yogesh. convert this raw JSON string to twitter4j.SatusJSONImpl
    var idindex =  message.indexOf("id=")
    var idtext =  message.indexOf("text=")

    InternalRow.fromSeq(Seq(UTF8String.fromString(message.substring(idindex+3, idtext-2)),
      UTF8String.fromString(message.substring(idtext+5, idtext+140))))
  }
}
