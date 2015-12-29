package org.apache.spark.sql.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by ymahajan on 25/09/15.
  */
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


  @transient val fileStream: DStream[String] = context.textFileStream(directory)
  // TODO: Yogesh, add support for other types of files streams

  stream = fileStream.flatMap(rowConverter.toRows)
}
