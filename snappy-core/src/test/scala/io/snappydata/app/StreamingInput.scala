package io.snappydata.app

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SnappyContext}
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds

object StreamingInput extends Serializable {

  val schemaString = "Year,Month,DayOfMonth,DayOfWeek,DepTime,CRSDepTime," +
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

  def main(args: Array[String]) {

    val conf = new org.apache.spark.SparkConf().
        setMaster("local-cluster[2,3,1024]").
        setAppName("StreamingInput")
    conf.setExecutorEnv(Seq(("extraClassPath",
        "/soubhikc1/wrk/w/s/experiments/SnappySparkTools/tests/target/scala-2.11/classes/")))
    val sc = new SparkContext(conf)

    val ssc = new org.apache.spark.streaming.StreamingContext(sc, Seconds(10))

    // hard coded server socket for debugging in eclipse
    val stream = ssc.socketTextStream("localhost", 9999,
      StorageLevel.MEMORY_AND_DISK_SER)

    val ingestionStream = stream.window(Seconds(10), Seconds(10))

    val snc = SnappyContext(sc)

    snc.registerSampleTable("arSample1", schema, Map(
      "qcs" -> "UniqueCarrier,Year,Month",
      "fraction" -> "0.02",
      "strataReservoirSize" -> "50",
      "timeInterval" -> "1m"))

    snc.registerSampleTable("arSample2", schema, Map(
      "qcs" -> "Year,Month",
      "fraction" -> "0.03",
      "strataReservoirSize" -> "100",
      "timeInterval" -> "10m"))

//    ingestionStream.saveStream(Seq("arSample1", "arSample2"),
//      userInterpreter.userDefinedRowInterpreter, schema)

    ssc.start()
    Thread.sleep(20000)

    future {

      snc.sql("SELECT count(*) FROM arSample1").show()

      Thread.sleep(10000)

      snc.sql("SELECT count(*) FROM arSample1").show()

      Thread.sleep(5000)

      snc.sql("SELECT count(*) FROM arSample1").show()

    } onSuccess { case ret => println("YAHOOOOO!!!!...." + ret) }

    Thread.sleep(5000) // sleep well otherwise stopping the stream will erase SAMPLING!!

    ssc.stop(stopSparkContext = false, stopGracefully = true)
    ssc.awaitTermination()
  }
}

object userInterpreter extends Serializable {
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
