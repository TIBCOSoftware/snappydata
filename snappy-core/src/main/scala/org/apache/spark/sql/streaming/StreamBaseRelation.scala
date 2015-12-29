package org.apache.spark.sql.streaming

import org.apache.spark.Logging
import org.apache.spark.rdd.{EmptyRDD, RDD, UnionRDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.sources.{BaseRelation, DeletableRelation, DestroyRelation, TableScan}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils

/**
  * Created by ymahajan on 25/09/15.
  */
abstract class StreamBaseRelation(options: Map[String, String]) extends BaseRelation with StreamPlan
with DeletableRelation with TableScan with DestroyRelation with Serializable with Logging {

  @transient val context = SnappyStreamingContext.getActive().get

  @transient var stream: DStream[InternalRow] = _

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
    if (stream.generatedRDDs.isEmpty) {
      new EmptyRDD[Row](sqlContext.sparkContext)
    } else {
      val rdds = Seq(stream.generatedRDDs.values.toArray: _*)
      val urdds = new UnionRDD(sqlContext.sparkContext, rdds)
      urdds.map(converter(_)).asInstanceOf[RDD[Row]]
    }
  }

  override def destroy(ifExists: Boolean): Unit = {
    throw new IllegalAccessException("Stream tables cannot be dropped")
  }

  override def delete(filterExpr: String): Int = {
    throw new IllegalAccessException("Stream table does not support deletes")
  }

  def truncate(): Unit = {
    throw new IllegalAccessException("Stream tables cannot be truncated")
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