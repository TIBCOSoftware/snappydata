package io.snappydata.hydra.ct

import java.io.{File, FileOutputStream, PrintWriter}
import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config

import org.apache.spark.sql.{SnappyContext, SnappyJobValid, SnappyJobValidation, SnappySQLJob}

object ValidateCTQueriesJob extends SnappySQLJob {

  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val outputFile = "ValidateCTQueries_" + jobConfig.getString("logFileName")
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    val tableType = jobConfig.getString("tableType")
    Try {
      snc.sql("set spark.sql.shuffle.partitions=23")
      CTQueries.snc = snc
      pw.println(s"Validate ${tableType} tables Queries Test started")
      CTTestUtil.executeQueries(snc, tableType, pw)
      pw.println(s"Validate ${tableType} tables Queries Test completed successfully")
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${outputFile}"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()
}
