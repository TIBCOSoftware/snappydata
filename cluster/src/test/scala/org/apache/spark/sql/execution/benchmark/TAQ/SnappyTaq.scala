package org.apache.spark.sql.execution.benchmark.TAQ

import java.io.{File, FileOutputStream, PrintStream}
import java.sql.{Date, Timestamp}
import java.util.{Calendar, GregorianCalendar, Random}

import com.typesafe.config.Config

import org.apache.spark.sql._
import org.apache.spark.sql.execution.benchmark.{Quote, Trade}
import org.apache.spark.sql.types.{Decimal, StringType, StructField, StructType}


/**
 * Created by kishor on 1/11/16.
 */
object SnappyTaq extends SnappySQLJob{

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

  val queries = Array(
    "select quote.sym, max(bid) from quote join S " +
        s"on (quote.sym = S.sym) where date='$d' group by quote.sym",
    "select trade.sym, ex, max(price) from trade join S " +
        s"on (trade.sym = S.sym) where date='$d' group by trade.sym, ex",
    "select trade.sym, hour(time), avg(size) from trade join S " +
        s"on (trade.sym = S.sym) where date='$d' group by trade.sym, hour(time)")

  override def runSnappyJob(session: SnappyContext, jobConfig: Config): Any = {

    import session.implicits._
   var queryPerfFileStream: FileOutputStream = new FileOutputStream(new File("SnappyTaq.out"))
   var queryPrintStream:PrintStream = new PrintStream(queryPerfFileStream)
   var avgPerfFileStream: FileOutputStream = new FileOutputStream(new File("Average.out"))
   var avgPrintStream:PrintStream = new PrintStream(avgPerfFileStream)

    session.dropTable("QUOTE", ifExists = true)
    session.dropTable("TRADE", ifExists = true)
    session.dropTable("S", ifExists = true)

    session.sql(s"${Taq.sqlQuote} using column")
    session.sql(s"${Taq.sqlTrade} using column")
    session.sql(s"CREATE TABLE S (sym CHAR(4) NOT NULL)")

    val quoteDF = session.range(quoteSize).mapPartitions { itr =>
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
    val tradeDF = session.range(tradeSize).mapPartitions { itr =>
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

    val quoteDataDF = session.internalCreateDataFrame(
      quoteDF.queryExecution.toRdd,
      StructType(quoteDF.schema.fields.map(_.copy(nullable = false))))
    val tradeDataDF = session.internalCreateDataFrame(
      tradeDF.queryExecution.toRdd,
      StructType(tradeDF.schema.fields.map(_.copy(nullable = false))))

    val sDF = session.createDataset(Taq.SYMBOLS)
    val symDF = session.internalCreateDataFrame(
      sDF.queryExecution.toRdd,
      StructType(Array(StructField("SYM", StringType, nullable = false))))


    quoteDataDF.write.insertInto("quote")
    tradeDataDF.write.insertInto("trade")

    //session.sql(s"insert into S select sym from trade where date=add_months($d,0) group by sym order by count(*) desc fetch first 100 rows only")
    symDF.write.insertInto("S")


    try {
       var queryCount = 0;
      for(query<- queries) {
        queryCount += 1

        println(s"Started executing $query")
        queryPrintStream.println(s"Started executing $query")
        if (isResultCollection) {
          val resultSet = session.sql(query).collect
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
            var cnts: Array[Row] = null
            if (i == 1) {
              cnts = session.sql(query).collect
            } else {
              cnts = session.sql(query).collect
            }
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

  private def collect(df: DataFrame): Unit = {
    val result = df.collect()
    // scalastyle:off
    println(s"Count = ${result.length}")
    // scalastyle:on
  }

  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = {
    path = if (config.hasPath("dataLocation")) {
      config.getString("dataLocation")
    } else {
      "/QASNAPPY/TPCH/DATA/1"
    }

    var sqlSparkProps = if (config.hasPath("sparkSqlProps")) {
      config.getString("sparkSqlProps")
    }
    else " "

    sqlSparkProperties = sqlSparkProps.split(",")

    isResultCollection = if (config.hasPath("resultCollection")) {
      config.getBoolean("resultCollection")
    } else {
      return new SnappyJobInvalid("Specify whether to to collect results")
    }

    warmUp = if (config.hasPath("warmUpIterations")) {
      config.getInt("warmUpIterations")
    } else {
      return new SnappyJobInvalid("Specify number of warmup iterations ")
    }
    runsForAverage = if (config.hasPath("actualRuns")) {
      config.getInt("actualRuns")
    } else {
      return new SnappyJobInvalid("Specify number of  iterations of which average result is calculated")
    }
    new SnappyJobValid()
    SnappyJobValid()
  }
}
