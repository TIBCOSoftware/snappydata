package org.apache.spark.sql.streaming

import org.apache.spark.Logging
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils

/**
  * Created by ymahajan on 25/09/15.
  */
abstract class StreamBaseRelation(options: Map[String, String]) extends BaseRelation with StreamPlan
with TableScan with Serializable with Logging {

  @transient val context = SnappyStreamingContext.getActive().get

  @transient var rowStream: DStream[InternalRow] = _

  val storageLevel = options.get("storageLevel")
      .map(StorageLevel.fromString)
      .getOrElse(StorageLevel.MEMORY_AND_DISK_SER_2)

  val rowConverter = {
    try {
      val clz = Utils.getContextOrSparkClassLoader.loadClass(options("rowConverter"))
      clz.newInstance().asInstanceOf[StreamToRowsConverter]
    } catch {
      case e: Exception => sys.error(s"Failed to load class : ${e.toString}")
    }
  }

  override def buildScan(): RDD[Row] = {

    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    if (rowStream.generatedRDDs.isEmpty) {
      new EmptyRDD[Row](sqlContext.sparkContext)
    } else {
      rowStream.generatedRDDs.maxBy(_._1)._2.map(converter(_)).asInstanceOf[RDD[Row]]
    }

    /* val currentRDD = rowStream.generatedRDDs.maxBy(_._1)._2
      val rdds = Seq(rowStream.generatedRDDs.values.toArray: _*)
      val urdds = new UnionRDD(sqlContext.sparkContext, rdds)
      urdds.map(converter(_)).asInstanceOf[RDD[Row]]
    } */
  }

}

private object StreamHelper {

  var validTime: Time = null

  def setValidTime(time: Time): Unit = {
    if (validTime == null) {
      validTime = time
    } else if (validTime != time) {
      validTime = time
    } else {
    }
  }
}