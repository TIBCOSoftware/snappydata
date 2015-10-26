package org.apache.spark.sql.streaming

import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by ymahajan on 25/09/15.
 */
class FileStreamSource extends SchemaRelationProvider{

  override def createRelation(sqlContext: SQLContext,
                              options: Map[String, String], schema: StructType) = {

    val DIRECTORY = "directory" // HDFS directory to monitor for new file
    val KEY = "key:" // Key type for reading HDFS file
    val VALUE = "value" //Value type for reading HDFS file
    val INPUT_FORMAT_HDFS = "inputformathdfs" //Input format for reading HDFS file

    val FILTER = "filter" //Function to filter paths to process
    val NEW_FILES_ONLY = "newfilesonly"  //Should process only new files and ignore existing files in the directory
    val CONF = "conf"  //Hadoop configuration

    val directory = options(DIRECTORY)

    val formatter = StreamUtils.loadClass(options("formatter")).newInstance() match {
      case f: StreamFormatter[_] => f.asInstanceOf[StreamFormatter[Any]]
      case f => throw new AnalysisException(s"Incorrect StreamFormatter $f")
    }
    val context = StreamingCtxtHolder.streamingContext
    val dStream = context.textFileStream(directory)
    dStream.foreachRDD {
      r => r.foreach(print)
    }
    FileStreamRelation(dStream.asInstanceOf[DStream[Any]], options, formatter.format, schema, sqlContext)
    //TODO: Yogesh, add support for other types of files streams
  }
}
