package org.apache.spark.sql.streaming

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{DeletableRelation, DestroyRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils

/**
 * Created by ymahajan on 25/09/15.
 */
case class FileStreamRelation(@transient val sqlContext: SQLContext,
                              options: Map[String, String],
                              override val schema: StructType)
  extends StreamBaseRelation with Logging with StreamPlan with Serializable {

  val DIRECTORY = "directory"
  // HDFS directory to monitor for new file
  val KEY = "key:"
  // Key type for reading HDFS file
  val VALUE = "value"
  //Value type for reading HDFS file
  val INPUT_FORMAT_HDFS = "inputformathdfs" //Input format for reading HDFS file

  val FILTER = "filter"
  //Function to filter paths to process
  val NEW_FILES_ONLY = "newfilesonly"
  //Should process only new files and ignore existing files in the directory
  val CONF = "conf" //Hadoop configuration

  val directory = options(DIRECTORY)

  @transient val context = StreamingCtxtHolder.streamingContext

  @transient val fileStream: DStream[String] = context.textFileStream(directory)
  //TODO: Yogesh, add support for other types of files streams

  private val streamToRow = {
    try {
      val clz = Utils.getContextOrSparkClassLoader.loadClass(options("streamToRow"))
      clz.newInstance().asInstanceOf[MessageToRowConverter]
    } catch {
      case e: Exception => sys.error(s"Failed to load class : ${e.toString}")
    }
  }

  @transient val stream: DStream[InternalRow] = fileStream.map(streamToRow.toRow)
}
