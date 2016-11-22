package io.snappydata.hydra

import java.io.{File, FileOutputStream, PrintWriter}

import io.snappydata.hydra.installJar.TestUtils
import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.{Failure, Success, Try}

/**
 * Created by swati on 22/11/16.
 */
object InstallJarTestSparkApp {
  val conf = new SparkConf().
    setAppName("InstallJarTest Application")
  val sc = new SparkContext(conf)
  val snc = SnappyContext(sc)

  def main(args: Array[String]): Unit = {
    val threadID = Thread.currentThread().getId
    val outputFile = "ValidateInstallJarTestApp_thread_" + threadID + "_" + System.currentTimeMillis + ".out"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    Try {
      pw.println("****** DynamicJarLoadingJob started ******")
      val currentDirectory: String = new File(".").getCanonicalPath
      val numServers: Int = args(1).toInt
      val expectedException: Boolean = args(2).toBoolean
      TestUtils.verify(snc, args(0), pw, numServers, expectedException)
      pw.println("****** DynamicJarLoadingJob finished ******")
      return String.format("See %s/" + outputFile, currentDirectory)

    } match {
      case Success(v) => pw.close()
      case Failure(e) =>
        pw.println(e.getMessage)
        pw.close();
        throw e;
    }
  }

}
