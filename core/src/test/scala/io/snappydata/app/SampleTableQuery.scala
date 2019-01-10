/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.app

import java.lang.management.ManagementFactory
import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}


object SampleTableQuery  extends Serializable {

  // var LINEITEM_DATA_FILE = "/Users/pmclain/Snappy/Data/tcph/tpch_data/1G/lineitem.tbl"
  var LINEITEM_DATA_FILE = "/Users/ashahid/workspace/snappy/experiments/BlinkPlay/data/datafile.tbl"
  //var warehouse = "/Users/pmclain/tmp/hive_warehouse"

  def msg(m: String) = DebugUtils.msg(m)

  def main(args: Array[String]): Unit = {
    try {
    //  val conf = new SparkConf().setAppName("BlinkDB Play").setMaster("spark://MacBook-Pro.local:7077")

      val conf = new SparkConf().setAppName("BlinkDB Play").setMaster("local[1]")
      conf.set("spark.sql.hive.metastore.sharedPrefixes","com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc,com.mapr.fs.shim.LibraryLoader,com.mapr.security.JNISecurity,com.mapr.fs.jni,org.apache.commons")
      conf.set("spark.sql.unsafe.enabled", "false")
      val sc = new SparkContext(conf)
      sc.addJar("/Users/ashahid/workspace/snappy/snappy-commons/snappy-core/build-artifacts/scala-2.10/classes/scala/test/app.jar")
      val spc = SnappyContext(sc)
     // val hiveContext = new HiveContext(spc)

      msg("created hive context")
      val numBatches = 1
      val numBootstrapTrials = 5 // keep small since we're debugging and printing...

      configureOnlineRun(spc, numBatches, numBootstrapTrials, "false") // Setup conf for BlinkDB
      msg("about to create table")
      createHiveLineitemTable(spc,"lineitem")
      msg("created table")
      // Just to make sure plain vanilla hive query works
      // showHiveQuery("select count(*) from lineitem", hiveContext)
      // showHiveQuery("SELECT avg(l_quantity) FROM lineitem", ctx)

      // val q = Array("select count(*) from lineitem")
      // val q = "select l_partkey, avg(l_extendedprice) from lineitem WHERE l_partkey < 10 GROUP BY l_partkey"
      val q = "SELECT sum(l_quantity) as T FROM lineitem"

      val df = spc.sql(q)



      msg(s"========= online query Plan for $q: ")
      // msg(ol.executedPlan.toString)

     df.show()
      val rows1 = df.collect()
      val sum = rows1(0).getDouble(0)
      /*assert(sum ==
        17*4 + 36*5 + 8*3 + 28 * 2 + 24*1 + 32 * 3 + 38 * 6 + 5* 45 + 4* 49 + 3* 27 + 2*2 + 1* 28 + 4*26)*/

     /* val struct = rows(0).getStruct(0)
      assert(struct.get(0) ==
        17*4 + 36*5 + 8*3 + 28 * 2 + 24*1 + 32 * 3 + 38 * 6 + 5* 45 + 4* 49 + 3* 27 + 2*2 + 1* 28 + 4*26

      )*/


      val mainTable = createHiveLineitemTable(spc,"mainTable")
     // val sampleTable = createHiveLineitemTable(spc,"sampleTable", true)



      spc.createSampleTable("mainTable_sampled", Some("mainTable"),
        Map(
          "qcs" -> "l_quantity",
          "fraction" -> "0.01",
          "strataReservoirSize" -> "50"),
          allowExisting = false)

      mainTable.write.insertInto("mainTable_sampled")

      //Run query on actual table
      val result = spc.sql("SELECT sum(l_quantity) as T FROM mainTable confidence 95")

      result.show()
      val rows2 = result.collect()
      val struct = rows2(0).getStruct(0)
      msg("estimate=" + struct.getDouble(0))
      msg("bound=" + struct.getDouble(1) + "," + struct.getDouble(2))
      msg(s"==== BlinkPlay::main(): END")



    }catch {
      case th :Throwable => th.printStackTrace()
    }


  }



  // Manages OLA fields in the hive context
  def configureOnlineRun(hc: SQLContext, numBatches: Int = 4,
      numBootstrap: Int = 100, debug: String = "false"): Unit = {
   //hc.setConf(NUM_WAVE_PER_BATCH, 10.toString) // Doesn't seem to do anything?
    // This seems to make a difference:  I get
    //
    // When I run WITHOUT lineitem marked STREAMED_RELATIONS:
    //
    //     [info] result for batch: [25.527660988782717]
    //
    // When I run WITH lineitem marked STREAMED_RELATIONS:
    //
    //     [info] result for batch: [[25.527660988782717,25.527660988782717,25.527660988782717]]
    //

  }

  def showHiveQuery(q: String, ctx: SQLContext): Unit = {
    ctx.sql(q).show()

  }

  /**
   * Creates an un-managed internal hive table for tpc-h lineitem info.
   * Loads data from LINEITEM_DATA_FILE (set in outer object)

  def createHiveLineitemTable(hiveContext: SQLContext): Unit = {
    msg(s"==== createHiveLineitemTable(): drop table if exists")
    hiveContext.sql("DROP TABLE IF EXISTS lineitem")

    msg(s"==== createHiveLineitemTable(): create table")
    val ddl = s"""
                 |CREATE TABLE lineitem (
                 |l_orderkey INT,
                 |l_partkey INT,
                 |l_suppkey INT,
                 |l_linenumber INT,
                 |l_quantity DOUBLE,
                 |l_extendedprice DOUBLE,
                 |l_discount DOUBLE,
                 |l_tax DOUBLE,
                 |l_returnflag STRING,
                 |l_linestatus STRING,
                 |l_shipdate STRING,
                 |l_commitdate STRING,
                 |l_receiptdate STRING,
                 |l_shipinstruct STRING,
                 |l_shipmode STRING,
                 |l_comment STRING)
      ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
    """.stripMargin.replaceAll("\n", " ")

    msg(s"==== createHiveLineitemTable(): run ddl")
    hiveContext.sql(ddl)

    msg(s"==== createHiveLineitemTable(): load table")
    hiveContext.sql(s"LOAD DATA LOCAL INPATH '$LINEITEM_DATA_FILE' INTO TABLE lineitem")

    msg(s"==== createHiveLineitemTable(): done")
  }*/

  def createHiveLineitemTable(hiveContext: SQLContext,
                              tableName: String, isSample: Boolean = false): DataFrame = {
    msg(s"==== createHiveLineitemTable(): drop table if exists")
    val schema = StructType(Seq(
      StructField("l_orderkey", IntegerType, false),
      StructField("l_partkey", IntegerType, false),
      StructField("l_suppkey", IntegerType, false),
      StructField("l_linenumber", IntegerType, false),
      StructField("l_quantity", FloatType, false),
      StructField("l_extendedprice", FloatType, false),
      StructField("l_discount", FloatType, false),
      StructField("l_tax", FloatType, false),
      StructField("l_returnflag", StringType, false),
      StructField("l_linestatus", StringType, false),
      StructField("l_shipdate", DateType, false),
      StructField("l_commitdate", DateType, false),
      StructField("l_receiptdate", DateType, false),
      StructField("l_shipinstruct", StringType, false),
      StructField("l_shipmode", StringType, false),
      StructField("l_comment", StringType, false),
      StructField("scale", IntegerType, false)
    ))

    hiveContext.sql("DROP TABLE IF EXISTS " + tableName)
    val people = hiveContext.sparkContext.textFile(LINEITEM_DATA_FILE).map(_.split('|')).map(p => Row(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt,p(3).trim.toInt,p(4).trim.toFloat,p(5).trim.toFloat,p(6).trim.toFloat,p(7).trim.toFloat,
      p(8).trim, p(9).trim,  java.sql.Date.valueOf(p(10).trim) , java.sql.Date.valueOf(p(11).trim), java.sql.Date.valueOf(p(12).trim), p(13).trim, p(14).trim, p(15).trim, p(16).trim.toInt ))


   val df =  if(isSample) {
     hiveContext.createDataFrame(hiveContext.sparkContext.emptyRDD[Row],
        schema)

    }else {
      val dfx = hiveContext.createDataFrame(people,schema)
      dfx.registerTempTable(tableName)
      dfx
    }

    msg(s"==== createHiveLineitemTable(): done")
    df
  }

/*
  // Non-Hive
  def createLineItemTable(sqlContext: SQLContext): DataFrame = {
    msg(s"==== createLineItemTable(): file=$LINEITEM_DATA_FILE")
    // The spark SQL DDL doesn't work (see comment at bottom of file),
    // and the CSV support seems buggy/lacking, so I explicitly make
    // a schema here...
    val schema = StructType(Seq(
      StructField("l_orderkey", IntegerType, false),
      StructField("l_partkey", IntegerType, false),
      StructField("l_suppkey", IntegerType, false),
      StructField("l_linenumber", IntegerType, false),
      StructField("l_quantity", FloatType, false),
      StructField("l_extendedprice", FloatType, false),
      StructField("l_discount", FloatType, false),
      StructField("l_tax", FloatType, false),
      StructField("l_returnflag", StringType, false),
      StructField("l_linestatus", StringType, false),
      StructField("l_shipdate", DateType, false),
      StructField("l_commitdate", DateType, false),
      StructField("l_receiptdate", DateType, false),
      StructField("l_shipinstruct", StringType, false),
      StructField("l_shipmode", StringType, false),
      StructField("l_comment", StringType, false)))

    msg(s"==== createLineItemTable(): schema created")

    val headers = "l_orderkey l_partkey l_suppkey l_linenumber l_quantity l_extendedprice l_discount l_tax l_returnflag l_linestatus l_shipdate l_commitdate l_receiptdate l_shipinstruct l_shipmode l_comment"

    val df = sqlContext.read.format("com.databricks.spark.csv")
        .options(Map("delimiter" -> "|",
          "headers" -> headers))
        .schema(schema)
        .load(LINEITEM_DATA_FILE)

    msg(s"==== createLineItemTable(): dataframe created")

    df.registerTempTable("lineitem")
    msg(s"==== createLineItemTable(): lineitem table created (DONE)")

    df
  }*/

}


/**
 * Debuggin Utilities.
 *
 * To get the di"..." string interpolator to work, you'll need to add this
 * import:
 *  import io.snappydata.util.DebugUtils._
 */
object DebugUtils {
  val format = new SimpleDateFormat("mm:ss:SSS")

  // Apparently, its pretty hard to get the PID of the current process in Java?!
  // Anyway, here is one method that depends on /proc, but I think we are all
  // running on platforms that have /proc.  If not, we'll have to redo this on to
  // use the Java ManagementFactory.getRuntimemMXBean() method?  See
  // http://stackoverflow.com/questions/35842/how-can-a-java-program-get-its-own-process-id
  //
  // This should probably be in a OS-specific class?
  //lazy val myPid: Int = Integer.parseInt(new File("/proc/self").getCanonicalFile().getName())

  lazy val myInfo: String = ManagementFactory.getRuntimeMXBean().getName()

  /**
   * Print a message on stdout but prefix with thread info and timestamp info
   */
  def msg(m: String): Unit = println(di"$m")

  /**
   * Get the PID for this JVM
   */
  def getPidInfo(): String = myInfo

  implicit class DebugInterpolator(val sc: StringContext) extends AnyVal {
    def di(args: Any*): String = {
      val ts = new Date(System.currentTimeMillis())
      s"==== [($myInfo) ${Thread.currentThread().getName()}: (${format.format(ts)})]:  ${sc.s(args:_*)}"
    }
  }
}
