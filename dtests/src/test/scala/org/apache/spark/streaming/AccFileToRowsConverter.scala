package org.apache.spark.streaming

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.StreamToRowsConverter


class AccFileToRowsConverter extends StreamToRowsConverter with Serializable {
  override def toRows(message: Any): Seq[Row] = {
    val record = message.asInstanceOf[String].split(",")
    val user_id = record{0}
    val ts = java.sql.Timestamp.valueOf(record{1})
    val ax = record{2}.toDouble
    val ay = record{3}.toDouble
    val az = record{4}.toDouble
    Seq(Row.fromSeq(Seq(user_id, ts, ax, ay, az)))
  }
}
