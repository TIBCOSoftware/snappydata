package org.apache.spark.sql.execution.benchmark.TAQ

import java.io.{File, FileOutputStream, PrintStream}
import java.sql.{DriverManager, ResultSet}

/**
 * Created by kishor on 1/11/16.
 */
object MemsqlTaq {

  val refQuote: String =
    s"""
       |CREATE TABLE quote (
       |   sym CHAR(4) NOT NULL,
       |   ex VARCHAR(64) NOT NULL,
       |   bid DOUBLE NOT NULL,
       |   time TIMESTAMP NOT NULL,
       |   date DATE NOT NULL
       |)
     """.stripMargin
//  KEY (sym) USING CLUSTERED COLUMNSTORE,
//  SHARD KEY (sym)
  val refTrade: String =
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

    def main(args: Array[String]) {

      val host = args(0)
      val port = args(1)
      val dbName = "TPCH"
      val user = "root"
      val password = ""

      var isResultCollection : Boolean = args(2).toBoolean
      var warmUp : Integer = args(3).toInt
      var runsForAverage : Integer = args(4).toInt

      Class.forName("com.mysql.jdbc.Driver")
      val dbAddress = "jdbc:mysql://" + host + ":" + port + "/"
      val conn = DriverManager.getConnection(dbAddress, user, password)
      val stmt = conn.createStatement
      println("KBKBKB : Creating Database")
      stmt.execute("DROP DATABASE IF EXISTS " + dbName)
      stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName)
      stmt.execute("USE " + dbName)

      println("KBKBKB : Creating Tables")
      stmt.execute(refQuote)
      stmt.execute(refTrade)
      //stmt.execute(s"CREATE REFERENCE TABLE S (sym CHAR(4) NOT NULL PRIMARY KEY)")
      stmt.execute(s"CREATE TABLE S (sym CHAR(4) NOT NULL)")

      println("Created Tables")

      println("KBKBKB : loading tables")
      stmt.execute("load data infile '/home/kishor/snappy/quote34.csv' into table quote FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
      stmt.execute("load data infile '/home/kishor/snappy/trade5.csv' into table trade  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
      stmt.execute("load data infile '/home/kishor/snappy/symbol.csv' into table S  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")

      val d="2016-06-06"

      val queries = Array(
        "select quote.sym, Max(bid) from quote join S " +
            s"on (quote.sym = S.sym) where date='$d' group by quote.sym",
        "select trade.sym, ex, Max(price) from trade join S " +
            s"on (trade.sym = S.sym) where date='$d' group by trade.sym, ex",
        "select trade.sym, hour(time), avg(size) from trade join S " +
            s"on (trade.sym = S.sym) where date='$d' group by trade.sym, hour(time)")

      var queryPerfFileStream: FileOutputStream = new FileOutputStream(new File("MemsqlTaq.out"))
      var queryPrintStream:PrintStream = new PrintStream(queryPerfFileStream)
      var avgPerfFileStream: FileOutputStream = new FileOutputStream(new File("Average.out"))
      var avgPrintStream:PrintStream = new PrintStream(avgPerfFileStream)

      var rs: ResultSet = null
      try {
        var queryCount = 0;
        for(query<- queries) {
          queryCount += 1

          println(s"Started executing $query")
          queryPrintStream.println(s"Started executing $query")
          if (isResultCollection) {
            rs = stmt.executeQuery(query)
            val rsmd = rs.getMetaData()
            val columnsNumber = rsmd.getColumnCount();
            var count : Int = 0
            while (rs.next()) {
              count += 1
              for (i:Int <- 1 to columnsNumber) {
                if (i > 1) queryPrintStream.print(",")
                queryPrintStream.print(rs.getString(i))
              }
              queryPrintStream.println()
            }
          } else {
            var totalTimeForLast5Iterations: Long = 0
            var totalTime: Long = 0
            var bestTime: Long=0
            for (i <- 1 to (warmUp + runsForAverage)) {
              val startTime = System.currentTimeMillis()
              rs = stmt.executeQuery(query)
              while (rs.next()) {
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
                totalTime += iterationTime
              }
            }
            queryPrintStream.println(s"${totalTime / runsForAverage}")
            avgPrintStream.println(s"$queryCount,$bestTime / ${totalTime /runsForAverage}")
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
      stmt.close();


    }

}
