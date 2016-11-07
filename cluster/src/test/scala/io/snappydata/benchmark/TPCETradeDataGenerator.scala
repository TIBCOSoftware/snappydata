package io.snappydata.benchmark

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, GregorianCalendar}

import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.types.Decimal
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by kishor on 21/10/16.
 */
object TPCETradeDataGenerator {

  def main (args: Array[String]): Unit = {
    val conf = new SparkConf()
          .setAppName("Datagenerator")
          .setMaster("local[1]")
          //.setMaster("snappydata://localhost:10334")
        .set("jobserver.enabled", "false")
    val sc = new SparkContext(conf)
    val snc =
      SnappyContext(sc)

    val quoteSize = 3400000
    val tradeSize = 500000

    val EXCHANGES: Array[String] = Array("NYSE", "NASDAQ", "AMEX", "TSE",
      "LON", "BSE", "BER", "EPA", "TYO")
    /*
    val SYMBOLS: Array[String] = Array("IBM", "YHOO", "GOOG", "MSFT", "AOL",
      "APPL", "ORCL", "SAP", "DELL", "RHAT", "NOVL", "HP")
    */
    val ALL_SYMBOLS: Array[String] = {
      val syms = new Array[String](400)
      for (i <- 0 until 10) {
        syms(i) = s"SY0$i"
      }
      for (i <- 10 until 100) {
        syms(i) = s"SY$i"
      }
      for (i <- 100 until 400) {
        syms(i) = s"S$i"
      }
      syms
    }
    val SYMBOLS: Array[String] = ALL_SYMBOLS.take(100)
    val numDays = 1
    import snc.implicits._

    val quoteDF = snc.range(quoteSize).mapPartitions { itr =>
      val rnd = new java.util.Random()
      val syms = ALL_SYMBOLS
      val numSyms = syms.length
      val exs = EXCHANGES
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
        val bid=rnd.nextDouble() * 100000
        //Quote(sym, ex, bid, new SimpleDateFormat("HH:mm:ss.SSS").format(time).toString, new SimpleDateFormat("yyyy-mm-dd").format(date).toString)
        Quote(sym, ex, bid, new SimpleDateFormat("HH:mm:ss.SSS").format(time).toString, date.toString)
      }
    }
    //quoteDF.write.format("com.databricks.spark.csv").option("dateFormat", "yyyy-MM-dd H:m:s").save("/home/kishor/quote.csv")
    quoteDF.write.format("com.databricks.spark.csv").save("/home/kishor/quote_Small.csv")

    val tradeDF = snc.range(tradeSize).mapPartitions { itr =>
      val rnd = new java.util.Random()
      val syms = ALL_SYMBOLS
      val numSyms = syms.length
      val exs = EXCHANGES
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
        val size=rnd.nextDouble() * 1000
        //Trade(sym, ex, dec, new SimpleDateFormat("HH:mm:ss.SSS").format(time).toString, new SimpleDateFormat("yyyy-mm-dd").format(date).toString, size)
        Trade(sym, ex, dec, new SimpleDateFormat("HH:mm:ss.SSS").format(time).toString, date.toString, size)
      }
    }
    tradeDF.write.format("com.databricks.spark.csv").save("/home/kishor/trade_Small.csv")

    val sDF = snc.createDataset(SYMBOLS)
    sDF.write.format("com.databricks.spark.csv").save("/home/kishor/symbol.csv")

  }

  case class Quote(sym: String, ex: String, bid: Double, time: String,
      date: String)

  case class Trade(sym: String, ex: String, price: String, time: String,
      date: String, size: Double)
}
