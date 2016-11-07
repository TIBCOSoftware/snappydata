package org.apache.spark.sql.execution.benchmark.TAQ

import java.sql.{Date, Timestamp}


/**
 * Created by kishor on 1/11/16.
 */
object Taq {

  case class Quote(sym: String, ex: String, bid: Double, time: Timestamp,
      date: Date)

  case class Trade(sym: String, ex: String, price: String, time: Timestamp,
      date: Date, size: Double)

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

  val sqlQuote: String =
    s"""
       |CREATE TABLE quote (
       |   sym CHAR(4) NOT NULL,
       |   ex VARCHAR(64) NOT NULL,
       |   bid DOUBLE NOT NULL,
       |   time TIMESTAMP NOT NULL,
       |   date DATE NOT NULL
       |)
     """.stripMargin
  val sqlTrade: String =
    s"""
       |CREATE TABLE trade (
       |   sym CHAR(4) NOT NULL,
       |   ex VARCHAR(64) NOT NULL,
       |   price DECIMAL(10,4) NOT NULL,
       |   time TIMESTAMP NOT NULL,
       |   date DATE NOT NULL,
       |   size DOUBLE NOT NULL
       |)
     """.stripMargin

//  case class StreamMessageQuoteObject(
//      sym:String,
//      ex:String,
//      bid:Double,
//      time:Timestamp,
//      date:Date
//      )
//
//  def  parseQuoteRow(s: Array[String]): StreamMessageQuoteObject = {
//    StreamMessageQuoteObject(
//      s(0),
//      s(1),
//      s(2).toDouble,
//      new Timestamp(s(3)),
//      formatDate(s(4))
//    )
//  }
}
