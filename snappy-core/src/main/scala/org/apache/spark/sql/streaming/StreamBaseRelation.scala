package org.apache.spark.sql.streaming

import org.apache.spark.Logging
import org.apache.spark.rdd.{EmptyRDD, RDD, UnionRDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
  * Created by ymahajan on 25/09/15.
  */
abstract class StreamBaseRelation(options: Map[String, String]) extends BaseRelation with StreamPlan
with TableScan with Serializable with Logging {

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
      // println("YOGS buildScan Empty " + stream.generatedRDDs) //scalastyle:ignore
      new EmptyRDD[Row](sqlContext.sparkContext)
    } else {
      // println("YOGS buildScan generatedRDDs " + stream.generatedRDDs) //scalastyle:ignore
      val rdds = Seq(stream.generatedRDDs.values.toArray: _*)
      // println("YOGS buildScan rdds " + rdds) //scalastyle:ignore
      val urdds = new UnionRDD(sqlContext.sparkContext, rdds)
      // println("YOGS buildScan map " + urdds.map(converter(_))) //scalastyle:ignore
      urdds.map(converter(_)).asInstanceOf[RDD[Row]]
    }
    /*
    context.sparkContext.parallelize((1 to 10).map(i => Row.fromSeq(Seq(i.toLong,
      UTF8String.fromString("text"),
      UTF8String.fromString("name"),
      UTF8String.fromString("lang"),
      i, UTF8String.fromString("hashtag")))))
     */
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