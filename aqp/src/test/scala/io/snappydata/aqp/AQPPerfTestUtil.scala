package io.snappydata.aqp

import java.io.PrintWriter
import org.apache.spark.sql.SnappyContext

/**
 * Created by supriya on 5/10/16.
 */
object AQPPerfTestUtil {

  def runPerftest(numIter: Int, snc: SnappyContext, pw: PrintWriter, queryArray: Array[String], skipTill: Integer, execTimeArray: Array[Double]): Unit = {
    pw.println("PerfTest will run for " + numIter + " iterations and first " + skipTill + " itertions will be skipped")
    val actualIter = numIter - skipTill
    for (i <- 1 to numIter) {
      snc.sql("set spark.sql.shuffle.partitions=6")
      pw.println("Starting iterration[" + i + "] and query length is " + queryArray.length)
      for (j <- 0 to queryArray.length - 1) {
        if (i < skipTill) {
          executeQuery(queryArray(j), 0)
        } else {
          execTimeArray(j) = executeQuery(queryArray(j), execTimeArray(j))
        }
      }
      pw.println()
    }

    def executeQuery(query: String, execTime: Double): Double = {
      val start = System.currentTimeMillis
      val actualResult = snc.sql(query)
      val result = actualResult.collect()
      var excTime = execTime
      val totalTime1 = (System.currentTimeMillis - start)
      pw.println(s"Query execution on " +
        s"table took  = ${totalTime1}ms")
      result.foreach(rs => {
        pw.println(rs.toString)
      })
      excTime += totalTime1
      return excTime
    }

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val pw1 = new PrintWriter("AvgExecutiontime.out")
    pw1.println("Inside the new file")
    //Calculate avg time taken to execute a particular query
    for (i <- 0 to queryArray.length - 1) {
      val meanQueryTime = execTimeArray(i) / (numIter - skipTill + 1)
      pw1.println(meanQueryTime)
      pw.println(s"Query ${queryArray(i)} took = ${meanQueryTime}ms as a mean over ${actualIter} iterrations")
      pw.println()
    }
    pw1.close()
    pw.println()
    pw.println(s"=====================================================")
    pw.close()
  }
}

