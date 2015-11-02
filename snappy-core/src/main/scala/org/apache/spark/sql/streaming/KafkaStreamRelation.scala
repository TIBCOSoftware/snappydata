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
case class KafkaStreamRelation[T](dStream: DStream[T],
                                  options: Map[String, Any],
                                  formatter: (RDD[T], StructType) => RDD[Row],
                                  override val schema: StructType,
                                  @transient override val sqlContext: SQLContext)
                                 (implicit val ct: ClassTag[T])
  extends BaseRelation with TableScan with Logging {

  override def buildScan(): RDD[Row] = {
    //dStream.map(_._2).map(formatter.format)
    throw new IllegalAccessException("Not Implemented")
  }
}
