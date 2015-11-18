package io.snappydata.core.aqp

import java.lang.management.ManagementFactory
import java.sql.Date
import java.text.SimpleDateFormat
import org.scalatest._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SQLContext, SnappyContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}
import org.apache.spark.sql.snappy._

/**
 * Created by ashahid on 11/17/15.
 */
class BootStrapAggregateFunctionTest extends FlatSpec with Matchers {

   //Set up sample & Main table
   var LINEITEM_DATA_FILE = "/Users/ashahid/workspace/snappy/experiments/BlinkPlay/data/datafile.tbl"

  val conf = new SparkConf().setAppName("BlinkDB Play").setMaster("local[1]")
  conf.set("spark.sql.hive.metastore.sharedPrefixes","com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc,com.mapr.fs.shim.LibraryLoader,com.mapr.security.JNISecurity,com.mapr.fs.jni,org.apache.commons")
  conf.set("spark.sql.unsafe.enabled", "false")
  val sc = new SparkContext(conf)
  //sc.addJar("/Users/ashahid/workspace/snappy/snappy-commons/snappy-core/build-artifacts/scala-2.10/classes/test/app.jar")
  val spc = SnappyContext(sc)
  createLineitemTable(spc,"lineitem")
  val mainTable = createLineitemTable(spc,"mainTable")
  spc.registerSampleTable("mainTable_sampled",
    mainTable.schema, Map(
      "qcs" -> "l_quantity",
      "fraction" -> 0.01,
      "strataReservoirSize" -> 50), Some("mainTable"))

  mainTable.insertIntoSampleTables("mainTable_sampled")


  "Sample Table Query on Sum aggregate " should "be correct" in {
    val result = spc.sql("SELECT sum(l_quantity) as T FROM mainTable confidence 95")

    result.show()
    val rows2 = result.collect()
    val struct = rows2(0).getStruct(0)
    msg("estimate=" + struct.getDouble(0))
    val estimate = struct.getDouble(0)
    assert( estimate === (17 + 36 + 8 + 28  + 24 + 32  + 38  +  45 +  49 + 27 + 2 + 28 + 26))
    msg("bound=" + struct.getDouble(1) + "," + struct.getDouble(2))

  }

  "Sample Table Query alias on Sum aggregate " should "be correct" in {
    val result = spc.sql("SELECT sum(l_quantity) as T FROM mainTable confidence 95")

    result.show()
    val rows2 = result.collect()
    val struct = rows2(0).getStruct(0)
    assert(rows2(0).schema.apply(0).name === "T")

  }

  def msg(m: String) = DebugUtils.msg(m)

  def createLineitemTable(sqlContext: SQLContext,
                              tableName: String, isSample: Boolean = false): DataFrame = {

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

    sqlContext.sql("DROP TABLE IF EXISTS " + tableName)

    val people = sqlContext.sparkContext.textFile(LINEITEM_DATA_FILE).map(_.split('|')).map(p => Row(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt,p(3).trim.toInt,p(4).trim.toFloat,p(5).trim.toFloat,p(6).trim.toFloat,p(7).trim.toFloat,
      p(8).trim, p(9).trim,  java.sql.Date.valueOf(p(10).trim) , java.sql.Date.valueOf(p(11).trim), java.sql.Date.valueOf(p(12).trim), p(13).trim, p(14).trim, p(15).trim, p(16).trim.toInt ))


    val df =  if(isSample) {
      sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row],
        schema)

    }else {
      val dfx = sqlContext.createDataFrame(people,schema)
      dfx.registerTempTable(tableName)
      dfx
    }

    df
  }

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