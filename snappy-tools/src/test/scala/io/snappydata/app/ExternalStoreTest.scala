package io.snappydata.app

import java.sql.DriverManager

import org.apache.spark.sql._
import org.apache.spark.sql.collection.ReusableRow
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.types._

object ExternalStoreTest extends App {

  def addArrDelaySlot(row: ReusableRow, arrDelayIndex: Int,
                      arrDelaySlotIndex: Int): Row = {
    val arrDelay =
      if (!row.isNullAt(arrDelayIndex)) row.getInt(arrDelayIndex) else 0
    row.setInt(arrDelaySlotIndex, math.abs(arrDelay) / 10)
    row
  }

  var hfile: String = getClass.getResource("/2015.parquet").getPath
  var loadData: Boolean = true
  var debug: Boolean = false
  var setJars, executorExtraClassPath: String = null
  var executorExtraJavaOptions: String = null
  var setMaster: String = "local[6]"

  val conf = new org.apache.spark.SparkConf().setAppName("ExternalStoreTest")
    .set("spark.logConf", "true")
    //.set("spark.shuffle.sort.serializeMapOutputs", "true")
    //.set("spark.executor.memory", "1g")
    //.set("spark.driver.memory", "1g")
  //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //.set("spark.shuffle.spill", "false")
  //.set("spark.shuffle.sort.bypassMergeThreshold", "-1")
  //.set("spark.sql.codegen", "true")
  //.set("spark.eventLog.enabled", "true")
  //.set("spark.eventLog.dir","file:/tmp/spark-events")
  //.set("spark.metrics.conf", "/home/sumedh/Projects/data/metrics.properties")
  if (setMaster != null) {
    //"local-cluster[3,2,1024]"
    conf.setMaster(setMaster)
  }

  if (setJars != null) {
    conf.setJars(Seq(setJars))
  }

  if (executorExtraClassPath != null) {
    // Intellij compile output path e.g. /wrk/out-snappy/production/MyTests/
    //conf.setExecutorEnv(Seq(("extraClassPath", executorExtraClassPath)))
    conf.set("spark.executor.extraClassPath", executorExtraClassPath)
  }

  if (debug) {
    val agentOpts = "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
    if (executorExtraJavaOptions != null) {
      executorExtraJavaOptions = executorExtraJavaOptions + " " + agentOpts
    } else {
      executorExtraJavaOptions = agentOpts
    }
  }

  if (executorExtraJavaOptions != null) {
    conf.set("spark.executor.extraJavaOptions", executorExtraJavaOptions)
  }

  // Set the url of the database
  // use these two when you want to test with snappydata database url
  // Alter the url property when creating the table below as well
  //conf.set("gemfirexd.db.url", "jdbc:snappydata:;mcast-port=45672;persist-dd=false;")
  //conf.set("gemfirexd.db.driver", "com.pivotal.gemfirexd.jdbc.EmbeddedDriver")

  var start: Long = 0
  var end: Long = 0
  var results: DataFrame = null
  var resultsC: Array[Row] = null

  val sc = new org.apache.spark.SparkContext(conf)
  val snContext = org.apache.spark.sql.SnappyContext(sc)
  snContext.sql("set spark.sql.shuffle.partitions=6")

  val props = Map(
    "url" -> "jdbc:gemfirexd:;mcast-port=33619;user=app;password=app;persist-dd=false",
    "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
    "poolImpl" -> "tomcat",
    "user" -> "app",
    "password" -> "app"
  )

  val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))

  val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
  val dataDF = snContext.createDataFrame(rdd)
  //dataDF.write.format("jdbc").options(props).saveAsTable("Order20")
  //dataDF.write.format("jdbc").mode(SaveMode.Append).options(props).saveAsTable("Order20")

  snContext.createColumnTable("ColumnTable53", dataDF.schema, "jdbcColumnar", props)

  dataDF.write.format("jdbcColumnar").mode(SaveMode.Append).options(props).saveAsTable("ColumnTable53")


  val result = snContext.sql("SELECT col1, col2 FROM ColumnTable53")

  result.collect.foreach(println)
  //snc.insert("ColumnTable2",dataDF);

  //
  //snc.createColumnTable("Order20","jdbc" , props)
  println("Successful")

}
