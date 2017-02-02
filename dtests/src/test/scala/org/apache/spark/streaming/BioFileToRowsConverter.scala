package org.apache.spark.streaming

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.StreamToRowsConverter

class BioFileToRowsConverter extends StreamToRowsConverter with Serializable {
  override def toRows(message: Any): Seq[Row] = {
    val record = message.asInstanceOf[String].split(",")
    val user_id = record{0}
    val ts = java.sql.Timestamp.valueOf(record{1})
    val heart_rate = record{2}.toInt
    val breath_rate = record{3}.toInt
    Seq(Row.fromSeq(Seq(user_id, ts, heart_rate, breath_rate)))
  }
}
