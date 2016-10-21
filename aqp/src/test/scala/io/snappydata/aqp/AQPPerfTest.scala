package io.snappydata.aqp

import java.io.{PrintWriter}
import scala.util.{Failure, Success, Try}
import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyContext, SnappyJobValidation, SnappySQLJob}

/**
 * Created by supriya on 4/10/16.
 */
object AQPPerfTest extends SnappySQLJob {

  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    val numIter = jobConfig.getString("numIter").toInt
    val skipTill = jobConfig.getString("skipTill").toInt
    val queryFile: String = jobConfig.getString("queryFile");
    val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
    val execTimeArray = new Array[Double](queryArray.length)

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val props = Map[String, String]()
    val pw = new PrintWriter("AQPPerfResults.out")
    Try {
      AQPPerfTestUtil.runPerftest(numIter, snc, pw, queryArray, skipTill, execTimeArray)
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/AQPPerfResults.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }
  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()
}


