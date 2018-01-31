package io.snappydata.benchmark.snappy.tpcds

import java.io.{File, FileOutputStream, PrintStream}

import org.apache.spark.Logging
import com.typesafe.config.Config
import io.snappydata.benchmark.TPCH_Queries
import io.snappydata.benchmark.snappy.tpch.QueryExecutor

import org.apache.spark.sql.catalyst.util.fileToString
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SnappyJobInvalid, SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

object QueryExecutionJob extends SnappySQLJob with Logging{
  var sqlSparkProperties: Array[String] = _
  var queries: Array[String] = _
  var queryPath: String = _
  var isResultCollection: Boolean = _
  var warmUp: Integer = _
  var runsForAverage: Integer = _

  def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext

    for (prop <- sqlSparkProperties) {
      snc.sql(s"set $prop")
    }

    val avgFileStream: FileOutputStream = new FileOutputStream(
      new File(s"Snappy_Average.out"))
    val avgPrintStream: PrintStream = new PrintStream(avgFileStream)

    queries.foreach { name =>
      try {

        val path: String = s"$queryPath/$name.sql"
        val queryString = fileToString(new File(path))

        val queryFileName = s"$name.out"

        val queryFileStream: FileOutputStream = new FileOutputStream(new File(queryFileName))
        val queryPrintStream: PrintStream = new PrintStream(queryFileStream)

        var totalTime: Long = 0

        // scalastyle:off println
        //println("Query : " + queryString)

        if (isResultCollection) {
          // queryPrintStream.println(queryToBeExecuted)
          val (resultSet, _) = QueryExecutor.queryExecution(name, queryString, snSession.sqlContext, true)
          println(s"$name : ${resultSet.length}")

          for (row <- resultSet) {
            queryPrintStream.println(row.toSeq.map {
              case d: Double => "%18.4f".format(d).trim()
              case v => v
            }.mkString(","))
          }
          println(s"$name Result Collected in file $queryFileName")
        }else {
          for (i <- 1 to (warmUp + runsForAverage)) {
            // queryPrintStream.println(queryToBeExecuted)
            val startTime = System.currentTimeMillis()
            var cnts: Array[Row] = null
            if (i == 1) {
              cnts = QueryExecutor.queryExecution(name, queryString, snSession.sqlContext, true)._1
            } else {
              cnts = QueryExecutor.queryExecution(name, queryString, snSession.sqlContext)._1
            }
            for (s <- cnts) {
              // just iterating over result
            }
            val endTime = System.currentTimeMillis()
            val iterationTime = endTime - startTime
            // scalastyle:off println
            queryPrintStream.println(s"$iterationTime")

            if (i > warmUp) {
              totalTime += iterationTime
            }
            cnts = null
          }
        }

        // scalastyle:off println
        //println(s"${totalTime / runsForAverage}")
        println("-----------------------------------------------")
        queryPrintStream.println(s"${totalTime / runsForAverage}")
        avgPrintStream.println(s"$name, executionTime = ${totalTime / runsForAverage}")
        println("-----------------------------------------------")

      }
      catch {
        case e: Exception => println(s"Failed $name  ");
          logError("Exception in job", e);
      }
    }
  }

  override def isValidJob(snSession: SnappySession, config: Config): SnappyJobValidation = {

    val sqlSparkProps = if (config.hasPath("sparkSqlProps")) {
      config.getString("sparkSqlProps")
    }
    else " "
    sqlSparkProperties = sqlSparkProps.split(",")

    val tempqueries = if (config.hasPath("queries")) {
      config.getString("queries")
    } else {
      return SnappyJobInvalid("Specify Query number to be executed")
    }
    // scalastyle:off println
    println(s"tempqueries : $tempqueries")
    queries = tempqueries.split(",")

    queryPath = if (config.hasPath("queryPath")) {
      config.getString("queryPath")
    } else {
      ""
    }

    isResultCollection = if (config.hasPath("resultCollection")) {
      config.getBoolean("resultCollection")
    } else {
      return SnappyJobInvalid("Specify whether to to collect results")
    }

    warmUp = if (config.hasPath("warmUpIterations")) {
      config.getInt("warmUpIterations")
    } else {
      return SnappyJobInvalid("Specify number of warmup iterations ")
    }
    runsForAverage = if (config.hasPath("actualRuns")) {
      config.getInt("actualRuns")
    } else {
      return SnappyJobInvalid("Specify number of  iterations of which average result is " +
          "calculated")
    }

    SnappyJobValid()
  }
}
