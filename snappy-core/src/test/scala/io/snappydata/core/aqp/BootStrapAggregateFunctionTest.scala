package io.snappydata.core.aqp

import java.lang.management.ManagementFactory
import java.sql.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, BoundReference, Expression}
import org.apache.spark.sql.execution.bootstrap.ApproxColumn
import org.scalatest._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SQLContext, SnappyContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.execution.Constants


/**
 * Created by ashahid on 11/17/15.
 */
class BootStrapAggregateFunctionTest extends FlatSpec with Matchers {

   //Set up sample & Main table
   var LINEITEM_DATA_FILE = "/Users/ashahid/workspace/snappy/experiments/BlinkPlay/data/datafile.tbl"


  var conf = createDefaultConf

  //sc.addJar("/Users/ashahid/workspace/snappy/snappy-commons/snappy-core/build-artifacts/scala-2.10/classes/test/app.jar")
  var spc = initTestTables(conf)

  private def createDefaultConf = {
    val conf = new SparkConf().setAppName("BlinkDB Play").setMaster("local[1]")
    conf.set("spark.sql.hive.metastore.sharedPrefixes","com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc,com.mapr.fs.shim.LibraryLoader,com.mapr.security.JNISecurity,com.mapr.fs.jni,org.apache.commons")
    conf.set("spark.sql.unsafe.enabled", "false")

    conf.set(Constants.keyNumBootStrapTrials, "25")
    conf
  }


  private def initTestTables(localConf: SparkConf): SQLContext = {
    val sc = new SparkContext(localConf)

    val snc = SnappyContext(sc)
    createLineitemTable(snc,"lineitem")
    val mainTable = createLineitemTable(snc,"mainTable")
    snc.registerSampleTable("mainTable_sampled",
      mainTable.schema, Map(
        "qcs" -> "l_quantity",
        "fraction" -> 0.01,
        "strataReservoirSize" -> 50), Some("mainTable"))

    mainTable.insertIntoSampleTables("mainTable_sampled")
    snc
  }

   behavior of "aggregate on sample table"

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

  "Sample Table Query with a given confidence " should "use correct quntiles" in {
    spc.sparkContext.stop()
    val numBootStrapTrials = 100
    conf = createDefaultConf
    val confidence = 90
    conf.set(Constants.keyAQPDebug, "true")
    conf.set(Constants.keyNumBootStrapTrials, numBootStrapTrials.toString)
    spc = initTestTables(conf)
    val result = spc.sql("SELECT sum(l_quantity) as T FROM mainTable confidence " + confidence)

    result.show()
    val rows = result.collect()
    assert(rows(0).schema.length === numBootStrapTrials)

    var i = 0
    val arrayOfBS = Array.fill[Double](numBootStrapTrials)( {
      val temp = i
      i = i+1
      rows(0).getDouble(temp)
    }
    )
    val estimate = arrayOfBS(0)
    val sortedData = arrayOfBS.sortWith( _.compareTo(_) <= 0 )
    val lowerBound = sortedData(9)
    val upperBound = sortedData(89)


    i = 0
    val arrayOfBSAny = Array.fill[Any](numBootStrapTrials)( {
      val temp = i
      i = i+1
      rows(0).getDouble(temp)
    }
    )

    i =0
    val columns = Array.fill[Expression](100)( {
      val temp = i
      i = i +1
      BoundReference(temp, DoubleType, false)

    }
    )

    i = 100
    val multiplicities = Array.fill[Expression](100)( {
      val temp = i
      i = i +1
      BoundReference(temp, ByteType, false)

    }
    )

    val arrayOfBytesAny = Array.fill[Any](100)(1.asInstanceOf[Byte])
    val arrayOfBoolAny = Array.fill[Any](1)(true)

    val approxColumn = ApproxColumn(confidence/100, columns, multiplicities,true)
    val internalRow = new GenericMutableRow(Array.concat[Any](arrayOfBSAny , arrayOfBytesAny, arrayOfBoolAny))
    val evalRow = approxColumn.eval(internalRow).asInstanceOf[InternalRow]
    assert(estimate == evalRow.getDouble(0))
    assert(lowerBound == evalRow.getDouble(1))
    assert(upperBound == evalRow.getDouble(2))


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
    sqlContext.sql("DROP TABLE IF EXISTS " + tableName + "_sampled" )

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