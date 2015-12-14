package io.snappydata.benchmark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SnappyContext

/**
 * Created by kishor on 19/10/15.
 */
class TPCHTable(val useOptionString: String, val prop:Map[String, String], val sparkContext:SparkContext) {

  val usingOptionString: String = useOptionString
  val props:Map[String, String] = prop
  val sc:SparkContext = sparkContext
  val snappyContext =  SnappyContext(sc)

}
