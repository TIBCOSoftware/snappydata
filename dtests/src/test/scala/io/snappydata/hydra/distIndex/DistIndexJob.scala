package io.snappydata.hydra.distIndex

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}

class DistIndexJob  extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val pw: PrintWriter = new PrintWriter(new FileOutputStream(new File(jobConfig.getString("logFileName"))), true)
    Try {
      pw.println("****** DistIndexJob started ******")
      pw.println("****** executeQueriesWithResultValidation task started ******")
      DistIndexTestUtils.executeQueriesWithResultValidation(snc, pw)
      pw.println("****** executeQueriesWithResultValidation task finished ******")
      pw.println("****** executeQueriesForBenchmarkResults task started ******")
      DistIndexTestUtils.executeQueriesForBenchmarkResults(snc, pw)
      pw.println("****** executeQueriesForBenchmarkResults task finished ******")
      pw.println("****** DistIndexJob finished ******")
      return String.format("See %s/" + jobConfig.getString("logFileName"), getCurrentDirectory)
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${jobConfig.getString("logFileName")}"
      case Failure(e) =>
        pw.println("Exception occurred while executing the job " + "\nError Message:" + e.getMessage)
        pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

}