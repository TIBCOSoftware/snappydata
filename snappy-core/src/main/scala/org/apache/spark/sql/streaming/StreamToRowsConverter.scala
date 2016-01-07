package org.apache.spark.sql.streaming

import org.apache.spark.sql.catalyst.InternalRow

trait StreamToRowsConverter extends Serializable {
  def toRows(message: Any): Seq[InternalRow]
}


