package io.snappydata.benchmark.memsql

import java.io.{PrintStream, FileOutputStream}
import java.sql.DriverManager
import java.util.Date

/**
 * Created by kishor on 21/7/16.
 */
object ConcurrentMemsql {



  def main(args: Array[String]): Unit = {

    val host = args(0)
    val port = 3306
    val dbName = "TPCH"
    val user = "root"
    val password = ""

    val readerThread = new Thread(new Runnable {
      def run() {
        Class.forName("com.mysql.jdbc.Driver")
        val dbAddress = "jdbc:mysql://" + host + ":" + port + "/"
        val conn = DriverManager.getConnection(dbAddress, user, password)
        val stmt = conn.createStatement
        stmt.execute("USE " + dbName)
        val avgFileStream = new FileOutputStream(new java.io.File(s"reader.out"))
        val avgPrintStream = new PrintStream(avgFileStream)
        for (i <- 1 to 100000) {

          var starttime = System.nanoTime()
          // val rs = stmt.executeQuery("select count(*) as counter   from PARTSUPP where ps_suppkey =  18692 and Ps_partkey = 7663535; ")
          val rs = stmt.executeQuery("select PS_AVAILQTY  as counter  from PARTSUPP where ps_suppkey =  18692 and PS_partkeY = 653535")
          var count = 0
          while (rs.next()) {
            count = rs.getInt("counter")
            //just iterating over result
            //count+=1
          }
          var timetaken = (System.nanoTime() - starttime)/1000

          avgPrintStream.println(s"Total time taken $timetaken  results : $count ${new Date()} ")

        }
        avgPrintStream.close()
      }
    }).start()

    val writerThread = new Thread(new Runnable {
      def run() {
        Class.forName("com.mysql.jdbc.Driver")
        val dbAddress = "jdbc:mysql://" + host + ":" + port + "/"
        val conn = DriverManager.getConnection(dbAddress, user, password)
        val stmt = conn.createStatement
        stmt.execute("USE " + dbName)
        val avgFileStream = new FileOutputStream(new java.io.File(s"writer.out"))
        val avgPrintStream = new PrintStream(avgFileStream)
        var startCounter = 7653535
        avgPrintStream.println(s"insertion started ${new Date()}")
        for (i <- 1 to 100000) {
          startCounter+=1
          try {
            var starttime = System.nanoTime()
            // val rs = stmt.execute(s"insert into PARTSUPP values ($startCounter, 18692 , 2, 4.11, 'aa') ")
            val rs = stmt.execute(s"update  PARTSUPP set  PS_AVAILQTY = PS_AVAILQTY +1")
          } catch {
            case e => avgPrintStream.println(e)
          }
        }

        avgPrintStream.println(s"insertion ended ${new Date()}")
        avgPrintStream.close()

      }

    }).start()
  }
}
