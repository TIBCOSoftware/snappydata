package org.apache.spark.sql.execution.benchmark.TAQ

import java.io.{File, FileOutputStream, PrintStream}

/**
 * Created by kishor on 1/11/16.
 */
object KDBTaq {

  val kdbQueries=Array(
    "select max bid by sym from quote where date=2016.06.06,sym in S",
    "select max price by sym,ex from trade where date=2016.06.06,sym in S",
    "select avg size  by sym,time.hh from trade where date=2016.06.06,sym in S"
  )

  var queryPerfFileStream: FileOutputStream = new FileOutputStream(new File("KdbTaq.out"))
  var queryPrintStream: PrintStream = new PrintStream(queryPerfFileStream)
  var avgPerfFileStream: FileOutputStream = new FileOutputStream(new File("KdbAverage.out"))
  var avgPrintStream: PrintStream = new PrintStream(avgPerfFileStream)

  def main(args: Array[String]): Unit = {
    val conn = new c("localhost", 5001)
    val warmUp= args(0).toInt
    val runsForAverage= args(1).toInt
    var queryCount = 0;
    for(query<- kdbQueries){
      queryCount += 1
      conn.k(query)
      var totalTimeForLast5Iterations: Long = 0
      queryPrintStream.println(queryCount)
      var bestTime: Long=0
      for (i <- 1 to (warmUp + runsForAverage)) {
        val startTime = System.currentTimeMillis()
        conn.k(query)
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
      }
      queryPrintStream.println(s"${totalTimeForLast5Iterations / runsForAverage}")
      avgPrintStream.println(s"$queryCount,$bestTime / ${totalTimeForLast5Iterations / runsForAverage}")
      println(s"Finished executing $queryCount")
    }

    conn.close()
  }
}
