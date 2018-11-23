package io.snappydata.hydra.putInto

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import io.snappydata.hydra.northwind.NWTestJob
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import scala.util.{Failure, Success, Try}

object ConcurrentPutInto extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val primaryLocatorHost = jobConfig.getString("primaryLocatorHost")
    val primaryLocatorPort = jobConfig.getString("primaryLocatorPort")
    val pw = new PrintWriter(new FileOutputStream(new File("ConcurrentPutIntoJob.out"), true))
    Try {
      ConcPutIntoTest.concPutInto(primaryLocatorHost, primaryLocatorPort)
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${NWTestJob.getCurrentDirectory}/ConcurrentPutIntoJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}


