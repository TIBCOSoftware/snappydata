package io.snappydata.aqp

import java.io.PrintWriter

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SnappyContext, SnappyJobValid, SnappyJobValidation, SnappySQLJob}

import scala.util.{Failure, Success, Try}

/**
 * Created by supriya on 7/10/16.
 * This job will load the actuall airline data  'numIter' times and store that data at the datalocation provided.
 */
object PopulateDataJob extends SnappySQLJob {
  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val dataLocation = jobConfig.getString("dataLocation")
    val pw = new PrintWriter("PopulateDataJob.out")
    Try {
      val tempTable = "tempcoltable"
      val numIter = 47
      var i : Double= 0
      while(i <= numIter) {
        val airlineDF1: DataFrame = snc.sql("select * from airline")
        airlineDF1.registerTempTable(tempTable)
        airlineDF1.write.format("column").mode(SaveMode.Append).saveAsTable("airline1")
        i = i + 1
        snc.dropTempTable(tempTable)
      }
      val df=snc.sql("SELECT * from airline1")
      df.write.parquet(dataLocation)
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/PopulateDataJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  /**
   * Validate if the data files are available, else throw SparkJobInvalid
   *
   */
  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()

}

