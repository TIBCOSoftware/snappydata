/**
 * Test for the insertIntoSampleTables and other streaming Snappy API.
 *
 * Created by Soubhik on 5/11/15.
 */
package io.snappydata.app

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.GregorianCalendar

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SnappyContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, Time}

case class StreamMessageObject(Year: Int,
    Month: Int,
    DayOfMonth: Int,
    DayOfWeek: Int,
    DepTime: Int,
    CRSDepTime: Int,
    ArrTime: Int,
    CRSArrTime: Int,
    UniqueCarrier: String,
    FlightNum: Int,
    TailNum: String,
    ActualElapsedTime: Int,
    CRSElapsedTime: Int,
    AirTime: Int,
    ArrDelay: Int,
    DepDelay: Int,
    Origin: String,
    Dest: String,
    Distance: Int,
    TaxiIn: Int,
    TaxiOut: Int,
    Cancelled: Int /*,
    CancellationCode: String,
    Diverted: Int,
    CarrierDelay: Int,
    WeatherDelay: Int,
    NASDelay: Int,
    SecurityDelay: Int,
    LateAircraftDelay: Int */)
    extends Serializable {
}

/**
 * Example 1: Use a 'case class' for auto inference of the schema.
 */
object StreamingInputV2 extends Serializable {

  def main(args: Array[String]) {

    val conf = new org.apache.spark.SparkConf().
        setMaster("local[3]"). // setMaster("local-cluster[3,1,512].
        setAppName("StreamingInputV2")
    conf.setExecutorEnv(Seq(("extraClassPath",
        "/soubhikc1/wrk/w/s/experiments/SnappySparkTools/tests/target/scala-2.11")))
    val sc = new org.apache.spark.SparkContext(conf)
    val ssc = new org.apache.spark.streaming.StreamingContext(sc, Seconds(10))

    // hard coded server socket for debugging in eclipse
    //val stream = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val stream = ssc.socketStream("localhost", 9999,
      myConverters.toObject[StreamMessageObject],
      StorageLevel.MEMORY_AND_DISK_SER)

    // val ingestionStream = stream //stream.window(Seconds(2), Seconds(2))
    val ingestionStream = stream.window(Seconds(10), Seconds(10))

    val snc = SnappyContext(sc)

    snc.registerTable[StreamMessageObject]("baseMessages")

    snc.registerSampleTableOn[StreamMessageObject]("messageObjectSampling_1", Map(
      "qcs" -> "UniqueCarrier,Year,Month",
      "fraction" -> "0.02",
      "strataReservoirSize" -> "100",
      "timeInterval" -> "1m"))

    snc.registerSampleTableOn[StreamMessageObject]("messageObjectSampling_2", Map(
      "qcs" -> "Year,Month",
      "fraction" -> "0.03",
      "strataReservoirSize" -> "200",
      "timeInterval" -> "10m"))

    val before1999 = Time(new GregorianCalendar(2000, 1, 1).getTimeInMillis)

    ingestionStream.foreachRDD((rdd: RDD[StreamMessageObject], time: Time) => {

      val df: DataFrame = snc.createDataFrame(rdd)

      // many user code

      df.appendToCache("baseMessages")

      if (time > before1999) {
        df.insertIntoAQPStructures("messageObjectSampling_1")
        df.filter("Year > 1999").insertIntoAQPStructures("messageObjectSampling_2")
      }
      else {
        df.insertIntoAQPStructures("messageObjectSampling_1",
          "messageObjectSampling_2")
      }

      // user code continues

    })

    ssc.start()

    future {

      Thread.sleep(20000)

      snc.sql("SELECT count(*) FROM arSample1").show()

      Thread.sleep(20000)

      snc.sql("SELECT count(*) FROM arSample2").show()

    } onComplete {
      case scala.util.Success(v) => println("YAHOOOOO!!!!...." + v)
      case scala.util.Failure(t) =>
        println("OHH!!!!... " + t)
        t.printStackTrace(System.out)
    }

    Thread.sleep(15000) // sleep well otherwise stopping the stream will erase SAMPLING!!

    ssc.stop(stopSparkContext = false, stopGracefully = true)
    ssc.awaitTermination()
  }
}


object myConverters extends Serializable {

  def toObject[T: ru.TypeTag : ClassTag](input: InputStream): Iterator[T] = {

    val dataInputStream = new BufferedReader(
      new InputStreamReader(input, "UTF-8"))

    toObject(new Iterator[String] {
      var line: String = _

      override def hasNext: Boolean = {
        line = dataInputStream.readLine()

        if (line == null) {
          dataInputStream.close()
        }
        line != null
      }

      override def next(): String = {
        line
      }
    })
  }

  private def getConstructorOf[T: ru.TypeTag : ClassTag] = {
    val mirror = ru.runtimeMirror(ru.typeOf[T].getClass.getClassLoader)
    val constructor = ru.typeOf[T].member(ru.nme.CONSTRUCTOR).asMethod

    (mirror.reflectClass(ru.typeOf[T].typeSymbol.asClass).reflectConstructor(
      constructor), constructor.paramss)
  }

  def toObject[T: ru.TypeTag : ClassTag](lines: Iterator[String]): Iterator[T] = {

    lazy val (constructor, constructorPrms) = getConstructorOf

    lines map { line =>
      val mapped = line.split(",").zipWithIndex.map({
        case (rstr, i)
          if constructorPrms.head(i).typeSignature =:= ru.typeOf[Int] =>
          if (rstr == "NA") 0 else rstr.toInt
        case (rstr, i)
          if constructorPrms.head(i).typeSignature =:= ru.typeOf[Double] =>
          rstr.toDouble
        case (rstr, i) => rstr.asInstanceOf[AnyRef]
      })

      try {
        constructor(mapped: _*).asInstanceOf[T]
      }
      catch {
        case t: Throwable => println(t)
          t.printStackTrace(System.out)
          throw t
      }
    }
  }
}

/*
    snc.sql("create *** " +
      "airline (" +
      " YearI INT NOT NULL," +
      " MonthI INT NOT NULL," +
      " DayOfMonth INT NOT NULL," +
      " DayOfWeek INT NOT NULL," +
      " DepTime INT," +
      " CRSDepTime INT," +
      " ArrTime INT," +
      " CRSArrTime INT," +
      " UniqueCarrier VARCHAR(20) NOT NULL," +
      " FlightNum INT," +
      " TailNum VARCHAR(20)," +
      " ActualElapsedTime INT," +
      " CRSElapsedTime INT," +
      " AirTime INT," +
      " ArrDelay INT," +
      " DepDelay INT," +
      " Origin VARCHAR(20)," +
      " Dest VARCHAR(20)," +
      " Distance INT," +
      " TaxiIn INT," +
      " TaxiOut INT," +
      " Cancelled INT," +
      " CancellationCode VARCHAR(20)," +
      " Diverted INT," +
      " CarrierDelay INT," +
      " WeatherDelay INT," +
      " NASDelay INT," +
      " SecurityDelay INT," +
      " LateAircraftDelay INT" +
      ")      " +
      "")
 */
