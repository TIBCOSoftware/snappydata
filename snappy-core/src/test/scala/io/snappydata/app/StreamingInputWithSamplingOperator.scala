/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
/**
 * Example of streaming input sampling.
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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.snappy._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, Time}


/**
 * EXAMPLE 1: Just input from stream a message object
 * and persist sampled information and base data into
 * a table.
 */
object StreamingInputWithSamplingOperator extends Serializable {

  type SamplingOptions = Seq[(String, Map[String, String])]

  def getSamplingConfiguration = {

    val baseTable = "airline_base"

    val samplingOpt_1 = ("airline_sample1",
        Map(
          "qcs" -> "UniqueCarrier,Year,Month",
          "fraction" -> "0.02",
          "strataReservoirSize" -> "100",
          "timeInterval" -> "1m"))

    val samplingOpt_2 = ("airline_sample2",
        Map(
          "qcs" -> "Year,Month",
          "fraction" -> "0.03",
          "strataReservoirSize" -> "200",
          "timeInterval" -> "10m"))

    (baseTable, Seq(samplingOpt_1, samplingOpt_2))
  }

  lazy val sc = {
    val conf = new org.apache.spark.SparkConf().
        setMaster("local[3]"). //setMaster("local-cluster[3,1,512]").
        setAppName("StreamingInputV2")
    conf.setExecutorEnv(Seq(("extraClassPath",
        "/soubhikc1/wrk/w/s/experiments/SnappySparkTools/tests/target/scala-2.11/classes/")))
    new org.apache.spark.SparkContext(conf)
  }

  def main(args: Array[String]) {
    val ssc = new org.apache.spark.streaming.StreamingContext(sc, Seconds(10))
    val stream = ssc.socketStream("localhost", 9999,
      myTextAndStreamConverters.toObject[StreamMessageObject],
      StorageLevel.MEMORY_AND_DISK_SER)
    val ingestionStream = stream.window(Seconds(10), Seconds(10))
    val before1999 = Time(new GregorianCalendar(2000, 1, 1).getTimeInMillis)
    val snc = SnappyContext(sc)

    val (loadData) = {
      if (args.length > 1) args(0).matches("-loaddata")
      else false
    }

    val (baseTable, samplingOptions) = getSamplingConfiguration

    snc.registerTable[StreamMessageObject](baseTable)
    samplingOptions foreach { case (sampleTable, _) =>
      snc.registerTable[StreamMessageObject](sampleTable)
    }

    if (loadData) {
      // Load data from file
      prefetchHistoricalData(baseTable, samplingOptions)
    }

    ingestionStream.foreachRDD((rdd: RDD[StreamMessageObject], time: Time) => {

      val df: DataFrame = snc.createDataFrame(rdd)

      // many user code

      df.appendToCache(baseTable)

      samplingOptions.foreach {
        case (sampleTable, opts) if sampleTable.endsWith("_sample1") =>
          df.stratifiedSample(opts).appendToCache(sampleTable)

        case (sampleTable, opts) if sampleTable.endsWith("_sample2") =>
          if (time > before1999) {
            df.filter("Year > 1999").stratifiedSample(opts).
                appendToCache(sampleTable)
          }
          else {
            df.stratifiedSample(opts).appendToCache(sampleTable)
          }
      }

    }) // END of foreachRDD

    ssc.start()

    // Parallel querying
    Future {

      for (i <- 1 to 3) {
        snc.sql(s"SELECT count(*) as $baseTable FROM $baseTable").show()
        samplingOptions foreach { case (sampleTable, _) =>
          snc.sql(s"SELECT count(*) as $sampleTable FROM $sampleTable").show()
        }

        try {
          Thread.sleep(20000)
        }
        catch {
          case t: Throwable => //ignore
        }
      }


    } onComplete {
      case scala.util.Success(v) => println("YAHOOOOO!!!!...." + v)
      case scala.util.Failure(t) =>
        println("OHH!!!!... " + t)
        t.printStackTrace(System.out)
    }

    ssc.stop(stopSparkContext = false, stopGracefully = true)

    println("querying before awaitTermination of stream")
    snc.sql(s"SELECT count(*) FROM $baseTable").show()

    ssc.awaitTermination()
  }

  def prefetchHistoricalData(baseTable: String,
      samplingOptions: SamplingOptions): Unit = {

    val snc = SnappyContext(sc)
    import snc.implicits._

    val baseDF1 = snc.read.parquet("/soubhikc1/wrk/w/s/data/2007-p5")

    def loadData(df: DataFrame, register: Boolean = false)
        (baseTable: String, samples: (String, Map[String, String])*): Unit = {
      if (register) {
        df.registerTempTable(baseTable)
      }

      samples foreach { case (sampleTable, options) =>
        val stratifiedFrame = df.stratifiedSample(options)
        if (register) stratifiedFrame.registerTempTable(sampleTable)
        stratifiedFrame.appendToCache(sampleTable)
      }
    }

    loadData(baseDF1, register = true)(baseTable, samplingOptions: _*)

    val hdfsData = snc.sparkContext.textFile(
      "/soubhikc1/wrk/w/s/data/2007-p4.csv")
    val baseDF2 = hdfsData.mapPartitions(
      myTextAndStreamConverters.toObject[StreamMessageObject]).toDF()

    loadData(baseDF2)(baseTable, samplingOptions: _*)
  }
}

case class UserTableOne(Year: Int,
    Month: Int,
    UniqueCarrier: String,
    FlightNum: Int,
    TailNum: String,
    ArrDelay: Int,
    DepDelay: Int,
    Origin: String,
    Dest: String)
    extends Serializable {
}

case class UserTableTwo(Year: Int,
    Month: Int,
    Delay: Int,
    Origin: String,
    Dest: String,
    Cancelled: Int)
    extends Serializable {
}

/**
 * EXAMPLE 2: Input a message object split it into multiple user tables
 * and persist them with and without sampling.
 */
object StreamingInputWithSamplingOperatorMessageSplit extends Serializable {

  def getTableRegistrations = {
    Seq(
      ("userTableOne", Seq("Sample1", "Sample2")),
      ("userTableOne", Seq("Sample1", "Sample2"))
    )
  }

  lazy val sc = {
    val conf = new org.apache.spark.SparkConf().
        setMaster("local[3]"). //setMaster("local-cluster[3,1,512].
        setAppName("StreamingInputV2")
    conf.setExecutorEnv(Seq(("extraClassPath",
        "/soubhikc1/wrk/w/s/experiments/SnappySparkTools/tests/target/scala-2.11/classes/")))
    new org.apache.spark.SparkContext(conf)
  }

  def main(args: Array[String]) {

    val ssc = new org.apache.spark.streaming.StreamingContext(sc, Seconds(10))
    val stream = ssc.socketStream("localhost", 9999,
      myTextAndStreamConverters.toObject[StreamMessageObject],
      StorageLevel.MEMORY_AND_DISK_SER)
    val ingestionStream = stream.window(Seconds(10), Seconds(10))

    val snc = SnappyContext(sc)
    import snc.implicits._

    def registerMyTables[T <: Product : ru.TypeTag](baseTable: String,
        sampleTables: String*): Unit = {
      snc.registerTable[T](baseTable)
      sampleTables foreach { p => snc.registerTable[T](p) }
    }

    val tables = getTableRegistrations

    registerMyTables[UserTableOne] _ tupled tables.head
    registerMyTables[UserTableTwo] _ tupled tables(1)

    ingestionStream.foreachRDD((rdd: RDD[StreamMessageObject], time: Time) => {

      import myTextAndStreamConverters.implicits

      tables.head match {
        case (table, sampleTables) =>
          val usrTable1 = rdd.toUserTableOne.toDF()

          //exact table persistence
          usrTable1.appendToCache(table)

          //sampled data persistence
          usrTable1.stratifiedSample(Map(
            "qcs" -> "Year, Month",
            "fraction" -> "0.01"
          )).appendToCache(sampleTables.head)

          usrTable1.stratifiedSample(Map(
            "qcs" -> "Year, Month, UniqueCarrier",
            "fraction" -> "0.04"
          )).appendToCache(sampleTables(1))
      }

      tables(1) match {
        case (table, sampleTables) =>
          val usrTable2 = rdd.toUserTableTwo.toDF()

          usrTable2.appendToCache(table)

          usrTable2.stratifiedSample(Map(
            "qcs" -> "Year, Month, Origin, Dest",
            "fraction" -> "0.01"
          )).appendToCache(sampleTables.head)

          usrTable2.filter("Cancelled = 0").stratifiedSample(Map(
            "qcs" -> "Month, Origin, Dest",
            "fraction" -> "0.01"
          )).appendToCache(sampleTables(1))
      }

      // user code continues

    }) // END of foreachRDD

    ssc.start()

    // Parallel querying
    Future {

      for (i <- 1 to 3) {
        tables foreach { case (table, sampleTables) =>
          snc.sql(s"SELECT count(*) as $table FROM $table").show()
          sampleTables foreach { sample =>
            snc.sql(s"SELECT count(*) as $sample FROM $sample").show()
          }
        }

        try {
          Thread.sleep(20000)
        }
        catch {
          case t: Throwable => //ignore
        }
      }
    } onComplete {
      case scala.util.Success(v) => println("YAHOOOOO!!!!...." + v)
      case scala.util.Failure(t) =>
        println("OHH!!!!... " + t)
        t.printStackTrace(System.out)
    }

    ssc.stop(stopSparkContext = false, stopGracefully = true)
    ssc.awaitTermination()
  }
}

object myTextAndStreamConverters extends Serializable {

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

  def toObject[T: ru.TypeTag : ClassTag](
      lines: Iterator[String]): Iterator[T] = {

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

  implicit class implicits(smoRdd: RDD[StreamMessageObject]) {

    def toUserTableOne: RDD[UserTableOne] =
      smoRdd.mapPartitions {
        _.map({
          case smo => new UserTableOne(smo.Year, smo.Month, smo.UniqueCarrier,
            smo.FlightNum, smo.TailNum, smo.ArrDelay, smo.DepDelay,
            smo.Origin, smo.Dest)
        })
      }

    def toUserTableTwo: RDD[UserTableTwo] = {
      smoRdd.mapPartitions {
        _.map({
          case smo => new UserTableTwo(smo.Year, smo.Month,
            smo.ArrDelay + smo.DepDelay, smo.Origin, smo.Dest, smo.Cancelled)
        })
      }
    }
  }

}

// end of converter

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
