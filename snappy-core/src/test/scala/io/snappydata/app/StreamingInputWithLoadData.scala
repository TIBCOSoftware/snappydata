/**
 * Created by soubhikc on 5/11/15.
 */
package io.snappydata.app

//import java.net.{ ServerSocket, Socket }

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SnappyContext, _}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

//import org.apache.spark.examples.streaming.Record

object StreamingInputWithLoadData extends Serializable {

  val schemaString = "Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime," +
    "ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime," +
    "CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn," +
    "TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay," +
    "WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay"

  val schemaTypes = List(IntegerType, IntegerType, IntegerType, IntegerType,
    IntegerType, IntegerType, IntegerType, IntegerType, StringType,
    IntegerType, StringType, IntegerType, IntegerType, IntegerType,
    IntegerType, IntegerType, StringType, StringType, IntegerType,
    IntegerType, IntegerType, IntegerType, StringType, IntegerType,
    IntegerType, IntegerType, IntegerType, IntegerType, IntegerType)

  val schema = StructType(schemaString.split(",").zipWithIndex.map {
    case (fieldName, i) => StructField(
      fieldName, schemaTypes(i), i >= 4)
  })

  def saveAsParquet(sqlContext: SQLContext) = {

    val airlinedata1 = sqlContext.sparkContext.textFile("/soubhikc1/wrk/w/s/data/2007-p5.csv")

    val rDF = sqlContext.createDataFrame(userTextAndStreamInterpreter.userDefinedRowInterpreter(airlinedata1, schema), schema)

    rDF.write.parquet("/soubhikc1/wrk/w/s/data/2007-p5")
  }

  def main(args: Array[String]) {


    //    val conf = new org.apache.spark.SparkConf().setMaster("local-cluster[3,3,1024]").setAppName("StreamingInput")
    val conf = new org.apache.spark.SparkConf().setMaster("local[4]").setAppName("StreamingInput")
    conf.setExecutorEnv(Seq(("extraClassPath", "/soubhikc1/wrk/w/s/experiments/SnappySparkTools/tests/target/scala-2.11/classes/")))
    val sc = new SparkContext(conf)
    val snc = SnappyContext(sc)

    //saveAsParquet(snc);

    val ssc = new org.apache.spark.streaming.StreamingContext(sc, Seconds(1))

    // hard coded server socket for debugging in eclipse
    val stream = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val ingestionStream = stream.window(Seconds(5), Seconds(5))


    import org.apache.spark.sql.snappy._

    val arS1 = snc.registerSampleTable("arSample1", schema, Map(
      "qcs" -> "UniqueCarrier,Year,Month",
      "fraction" -> "0.02",
      "strataReservoirSize" -> "50",
      "timeInterval" -> "1m"))

    snc.registerSampleTable("arSample2", schema, Map(
      "qcs" -> "Year,Month",
      "fraction" -> "0.03",
      "strataReservoirSize" -> "100",
      "timeInterval" -> "10m"))

    // initial load of data.
    val baseDF1 = snc.read.parquet("/soubhikc1/wrk/w/s/data/2007-p5")
    baseDF1.insertIntoAQPStructures("arSample1", "arSample2")

    snc.sql("SELECT count(*) FROM arSample1").show

    val basedata = snc.sparkContext.textFile("/soubhikc1/wrk/w/s/data/2007-p4.csv")
    val baseDF2 = snc.createDataFrame(userInterpreter.userDefinedRowInterpreter(basedata, schema), schema)

    baseDF2.insertIntoAQPStructures("arSample1", "arSample2")

    snc.sql("SELECT count(*) FROM arSample1").show

//    ingestionStream.saveStream(Seq("arSample1", "arSample2"),
//      userInterpreter.userDefinedRowInterpreter, schema)

    ssc.start()
    Thread.sleep(5000)

    //    future {

    snc.sql("SELECT Year, Month FROM arSample1").show

    Thread.sleep(10000)

    snc.sql("SELECT Year, Month FROM arSample1").show

    Thread.sleep(5000)

    snc.sql("SELECT Year, Month FROM arSample1").show

    //      results = snc.sql("SELECT count(*) FROM arSample2")
    //      results.map(t => t(0)).collect().foreach(println)

    //    } onSuccess { case ret => println("YAHOOOOO!!!!...." + ret) }

    Thread.sleep(35000) // sleep well otherwise stopping the stream will erase SAMPLING!!
    println("About to STOP......")


    ssc.stop(false, true)
    ssc.awaitTermination()
    //    println("SUM " + sum)
    //    println("EXITING")
  }
}

object userTextAndStreamInterpreter extends Serializable {
  def userDefinedRowInterpreter[R <: RDD[String]](r: R, schema: StructType) = {
    r.map(_.split(",")).map(p => {
      Row.fromSeq(p.zipWithIndex.map {
        case (v, i) =>
          if (schema(i).dataType == IntegerType) {
            if (v == "NA") null else v.toInt
          } else v
      })
    })
  }
}


/*

    var hfile1: String = "/soubhikc1/wrk/w/s/data/2007-p1.csv"

    val airlinedata1 = sc.textFile(hfile1)
    val rowRDD1 = userDefinedRowInterpreter(airlinedata1)
    val airlineSchemaRDD: DataFrame = sqlContext.createDataFrame(rowRDD1, schema)
    airlineSchemaRDD.saveAsParquetFile("/soubhikc1/temp/p1")


    airlineSchemaRDD.registerTempTable("airline")
    sqlContext.cacheTable("airline")

    var results: DataFrame = null
    results = sqlContext.sql("SELECT count(*) FROM airline")
    results.map(t => t(0)).collect().foreach(println)



      // Get the singleton instance of SQLContext
//      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
//            import sqlContext.implicits._

      val c = rdd.count();
      println(new java.util.Date(System.currentTimeMillis()) + " " + c)
      sum += c

      results = sqlContext.sql("SELECT count(*) FROM airline")
      results.map(t => t(0)).collect().foreach(println)

      println("DONE..................")

*/
