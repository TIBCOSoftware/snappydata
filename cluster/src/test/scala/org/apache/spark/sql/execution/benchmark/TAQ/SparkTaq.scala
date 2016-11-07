package org.apache.spark.sql.execution.benchmark.TAQ

import java.io.{File, FileOutputStream, PrintStream}
import java.sql.{Date, Timestamp}
import java.util.{Calendar, GregorianCalendar, Random}

import org.apache.spark.sql.execution.benchmark.{Quote, Trade}
import org.apache.spark.sql.types.{Decimal, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by kishor on 1/11/16.
 */
object SparkTaq {

  var path: String = _
  val quoteSize = 34000000L
  val tradeSize = 5000000L
  val numDays = 1
  val d="2016-06-06"

  var sqlSparkProperties: Array[String] = _
  var isResultCollection: Boolean = _
  var isSnappy: Boolean = true
  var warmUp: Integer = _
  var runsForAverage: Integer = _

  val cacheQueries = Array(
    "select cQuote.sym, max(bid) from cQuote join cS " +
        s"on (cQuote.sym = cS.sym) where date='$d' group by cQuote.sym",
    "select cTrade.sym, ex, max(price) from cTrade join cS " +
        s"on (cTrade.sym = cS.sym) where date='$d' group by cTrade.sym, ex",
    "select cTrade.sym, hour(time), avg(size) from cTrade join cS " +
        s"on (cTrade.sym = cS.sym) where date='$d' group by cTrade.sym, hour(time)"
  )

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkTaq") /*.set("snappydata.store.locators","localhost:10334")*/


    val sc = new SparkContext(conf)
    val snc = new SQLContext(sc)

    val spark = SparkSession
        .builder()
        .appName("SparkTaq")
        .getOrCreate()

    val path = args(0)
    val queries = args(1).split("-")
    var isResultCollection: Boolean = args(2).toBoolean
    warmUp= args(3).toInt
    runsForAverage= args(4).toInt
    var sqlSparkProperties = args(5).split(",")
    var queryPerfFileStream: FileOutputStream = new FileOutputStream(new File("SparkTaq.out"))
    var queryPrintStream: PrintStream = new PrintStream(queryPerfFileStream)
    var avgPerfFileStream: FileOutputStream = new FileOutputStream(new File("SparkAverage.out"))
    var avgPrintStream: PrintStream = new PrintStream(avgPerfFileStream)

    import spark.implicits._

    val quoteDF = spark.range(quoteSize).mapPartitions { itr =>
      val rnd = new Random
      val syms = Taq.ALL_SYMBOLS
      val numSyms = syms.length
      val exs = Taq.EXCHANGES
      val numExs = exs.length
      var day = 0
      // month is 0 based
      var cal = new GregorianCalendar(2016, 5, day + 6)
      var date = new Date(cal.getTimeInMillis)
      var dayCounter = 0
      itr.map { id =>
        val sym = syms(rnd.nextInt(numSyms))
        val ex = exs(rnd.nextInt(numExs))
        if (numDays > 1) {
          dayCounter += 1
          // change date after some number of iterations
          if (dayCounter == 10000) {
            day = (day + 1) % numDays
            cal = new GregorianCalendar(2016, 5, day + 6)
            date = new Date(cal.getTimeInMillis)
            dayCounter = 0
          }
        }
        cal.set(Calendar.HOUR, rnd.nextInt(8))
        cal.set(Calendar.MINUTE, rnd.nextInt(60))
        cal.set(Calendar.SECOND, rnd.nextInt(60))
        cal.set(Calendar.MILLISECOND, rnd.nextInt(1000))
        val time = new Timestamp(cal.getTimeInMillis)
        Quote(sym, ex, rnd.nextDouble() * 100000, time, date)
      }
    }
    val tradeDF = spark.range(tradeSize).mapPartitions { itr =>
      val rnd = new Random
      val syms = Taq.ALL_SYMBOLS
      val numSyms = syms.length
      val exs = Taq.EXCHANGES
      val numExs = exs.length
      var day = 0
      // month is 0 based
      var cal = new GregorianCalendar(2016, 5, day + 6)
      var date = new Date(cal.getTimeInMillis)
      var dayCounter = 0
      itr.map { id =>
        val sym = syms(rnd.nextInt(numSyms))
        val ex = exs(rnd.nextInt(numExs))
        if (numDays > 1) {
          dayCounter += 1
          // change date after some number of iterations
          if (dayCounter == 10000) {
            // change date
            day = (day + 1) % numDays
            cal = new GregorianCalendar(2016, 5, day + 6)
            date = new Date(cal.getTimeInMillis)
            dayCounter = 0
          }
        }
        cal.set(Calendar.HOUR, rnd.nextInt(8))
        cal.set(Calendar.MINUTE, rnd.nextInt(60))
        cal.set(Calendar.SECOND, rnd.nextInt(60))
        cal.set(Calendar.MILLISECOND, rnd.nextInt(1000))
        val time = new Timestamp(cal.getTimeInMillis)
        val dec = Decimal(rnd.nextInt(100000000), 10, 4).toString
        Trade(sym, ex, dec, time, date, rnd.nextDouble() * 1000)
      }
    }

    val quoteDataDF = spark.internalCreateDataFrame(
      quoteDF.queryExecution.toRdd,
      StructType(quoteDF.schema.fields.map(_.copy(nullable = false))))
    val tradeDataDF = spark.internalCreateDataFrame(
      tradeDF.queryExecution.toRdd,
      StructType(tradeDF.schema.fields.map(_.copy(nullable = false))))

    val sDF = spark.createDataset(Taq.SYMBOLS)
    val symDF = spark.internalCreateDataFrame(
      sDF.queryExecution.toRdd,
      StructType(Array(StructField("SYM", StringType, nullable = false))))

    println("KBKBKB : Creating table")
    quoteDataDF.createOrReplaceTempView("cQuote")
    tradeDataDF.createOrReplaceTempView("cTrade")
    symDF.createOrReplaceTempView("cS")
    println("KBKBKB : Created temp table")

    //SnappyAggregation.enableOptimizedAggregation = false
    spark.catalog.cacheTable("cQuote")
    spark.catalog.cacheTable("cTrade")
    spark.catalog.cacheTable("cS")
    println("KBKBKB : Cache table")

//    var sqlDF = spark.sql("SELECT * FROM cS")
//    sqlDF.show()
//
//    sqlDF = spark.sql("SELECT count(*) FROM cQuote")
//    sqlDF.show()
//
//    sqlDF = spark.sql("SELECT count(*) FROM cTrade")
//    sqlDF.show()
//
//
//    Thread.sleep(50000)
    try {
      var queryCount = 0;
      for (query <- cacheQueries) {
        queryCount += 1

        println(s"Started executing $query")
        queryPrintStream.println(s"Started executing $query")
        if (isResultCollection) {
          val resultSet = spark.sql(query).collect
          println(s"$query : ${resultSet.length}")

          for (row <- resultSet) {
            queryPrintStream.println(row.toSeq.map {
              case d: Double => "%18.4f".format(d).trim()
              case v => v
            }.mkString(","))
          }
          queryPrintStream.println()
        } else {
          var totalTimeForLast5Iterations: Long = 0
          var bestTime: Long=0
          queryPrintStream.println(queryCount)
          for (i <- 1 to (warmUp + runsForAverage)) {
            val startTime = System.currentTimeMillis()
            var cnts: Array[Row] = spark.sql(query).collect
            for (s <- cnts) {
              //just iterating over result
            }
            val endTime = System.currentTimeMillis()
            val iterationTime = endTime - startTime
            if(i==1){
              bestTime = iterationTime
            }else{
              if(iterationTime < bestTime)
                bestTime = iterationTime
            }
            queryPrintStream.println(s"$iterationTime")
            if (i > warmUp) {
              totalTimeForLast5Iterations += iterationTime
            }
            cnts = null
          }
          queryPrintStream.println(s"${totalTimeForLast5Iterations / runsForAverage}")
          avgPrintStream.println(s"$queryCount,$bestTime / ${totalTimeForLast5Iterations /runsForAverage}")
          println(s"Finished executing $queryCount")
        }
      }
    } catch {
      case e: Exception => {
        e.printStackTrace(queryPrintStream)
        e.printStackTrace(avgPrintStream)
        println(s" Exception while executing Taq Query")
      }
    } finally {
      queryPrintStream.close()
      queryPerfFileStream.close()
    }
  }
}
