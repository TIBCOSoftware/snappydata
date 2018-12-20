package io.snappydata.hydra.spva

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import io.snappydata.hydra.spva
import org.apache.spark.SparkContext
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}


object ValidateSPVAQueriesJob extends SnappySQLJob {
  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    val snc = snappySession.sqlContext

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val tableType = jobConfig.getString("tableType")
    val outputFile = "ValidateSPVAQueries_" + tableType + "_" + jobConfig.getString("logFileName")
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
    Try {
      val dataFilesLocation = jobConfig.getString("dataFilesLocation")
      snc.setConf("dataFilesLocation", dataFilesLocation)
      spva.SPVAQueries.snc = snc
      SPVAQueries.dataFilesLocation = dataFilesLocation
      pw.println(s"createAndLoadSparkTables Test started at : " + System.currentTimeMillis)
      sqlContext.sql("CREATE SCHEMA IF NOT EXISTS SPD")
      SPVATestUtil.createAndLoadSparkTables(sqlContext)
      println(s"createAndLoadSparkTables Test completed successfully at : " + System
          .currentTimeMillis)
      pw.println(s"createAndLoadSparkTables Test completed successfully at : " + System
          .currentTimeMillis)
      pw.println(s"ValidateQueriesFullResultSet for ${tableType} tables Queries Test started at" +
          s" :  " + System.currentTimeMillis)
      SPVATestUtil.validateQueriesFullResultSet(snc, tableType, pw, sqlContext)
      pw.println(s"validateQueriesFullResultSet ${tableType} tables Queries Test completed  " +
          s"successfully at : " + System.currentTimeMillis)

      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${outputFile}"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}

