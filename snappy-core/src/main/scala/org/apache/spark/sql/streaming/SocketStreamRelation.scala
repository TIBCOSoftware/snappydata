package org.apache.spark.sql.streaming

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
 * Created by ymahajan on 25/09/15.
 */
case class SocketStreamRelation[T](dStream: DStream[T],
                                   options: Map[String, Any],
                                   formatter: (RDD[T], StructType) => RDD[Row],
                                   override val schema: StructType,
                                   @transient override val sqlContext: SQLContext)
                                  (implicit val ct: ClassTag[T])
  extends BaseRelation with TableScan with Logging {

  override def buildScan(): RDD[Row] = {
    null
    // Example : how to read from a csv file
    /*val rdd = sqlContext.sparkContext.textFile("location")
    val firstLine = null
    val dataLines = rdd.filter(row => row != firstLine)
    val rowRDD = dataLines.map(row => {
      val columnValues = row.split(",")
      Row.fromSeq(columnValues)
    })
    rowRDD*/
  }
}