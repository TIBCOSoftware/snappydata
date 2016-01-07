package org.apache.spark.sql.streaming

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream

final class FileStreamSource extends SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String],
      schema: StructType): BaseRelation = {
    new FileStreamRelation(sqlContext, options, schema)
  }
}

case class FileStreamRelation(@transient val sqlContext: SQLContext,
    options: Map[String, String],
    override val schema: StructType)
    extends StreamBaseRelation(options) {

  // HDFS directory to monitor for new file
  val DIRECTORY = "directory"

  // HDFS directory to monitor for new file
  val KEY = "key:"

  // Value type for reading HDFS file
  val VALUE = "value"

  // Input format for reading HDFS file
  val INPUT_FORMAT_HDFS = "inputformathdfs"

  // Function to filter paths to process
  val FILTER = "filter"

  // Should process only new files and ignore existing files in the directory
  val NEW_FILES_ONLY = "newfilesonly"

  // Hadoop configuration
  val CONF = "conf"

  val directory = options(DIRECTORY)

  // TODO: Yogesh, add support for other types of files streams

  if (FileStreamRelation.getRowStream() == null) {
    rowStream = {
      context.textFileStream(directory).flatMap(rowConverter.toRows)
    }
    FileStreamRelation.setRowStream(rowStream)
    // TODO Yogesh, this is required from snappy-shell, need to get rid of this
    rowStream.foreachRDD { rdd => rdd }
  } else {
    rowStream = FileStreamRelation.getRowStream()
  }
}

object FileStreamRelation extends Logging {
  private var rowStream: DStream[InternalRow] = null

  private val LOCK = new Object()

  private def setRowStream(stream: DStream[InternalRow]): Unit = {
    LOCK.synchronized {
      rowStream = stream
    }
  }

  private def getRowStream(): DStream[InternalRow] = {
    LOCK.synchronized {
      rowStream
    }
  }
}
