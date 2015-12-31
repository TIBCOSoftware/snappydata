package io.snappydata

import java.io.File
import java.sql.SQLException

import scala.util.control.NonFatal

import io.snappydata.core.{FileCleaner, LocalSparkConf}

import org.apache.spark.sql.collection.ToolsCallbackInit

//scalastyle:off
import org.scalatest.{BeforeAndAfterAll, FunSuite, Outcome}
//scalastyle:on

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Outcome}

import scala.collection.mutable.ArrayBuffer

/**
 * Base abstract class for all SnappyData tests similar to SparkFunSuite.
 *
 * Created by soubhikc on 6/10/15.
 */
abstract class SnappyFunSuite
    extends FunSuite // scalastyle:ignore
    with BeforeAndAfterAll
    with Logging {

  protected var testName: String = _
  protected var dirList = ArrayBuffer[String]()

  protected def sc: SparkContext = {
    val ctx = SnappyContext.globalSparkContext
    if (ctx != null && !ctx.isStopped) ctx
    else new SparkContext(newSparkConf())
  }

  protected def sc(addOn: (SparkConf) => SparkConf): SparkContext = {
    val ctx = SnappyContext.globalSparkContext
    if (ctx != null && !ctx.isStopped) ctx
    else new SparkContext(newSparkConf(addOn))
  }

  protected def scWithConf(addOn: (SparkConf) => SparkConf): SparkContext = {
    new SparkContext(newSparkConf(addOn))
  }

  protected def snc: SnappyContext = SnappyContext.getOrCreate(sc)

  /**
   * Copied from SparkFunSuite.
   *
   * Log the suite name and the test name before and after each test.
   *
   * Subclasses should never override this method. If they wish to run
   * custom code before and after each test, they should mix in the
   * {{org.scalatest.BeforeAndAfter}} trait instead.
   */
  final protected override def withFixture(test: NoArgTest): Outcome = {
    testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("io.snappydata", "i.sd").
        replaceAll("org.apache.spark", "o.a.s")
    try {
      logInfo(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      test()
    } finally {
      logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }

  def deleteDir(dir: String): Boolean = {
    FileCleaner.deletePath(dir)
  }

  protected def newSparkConf(addOn: (SparkConf) => SparkConf = null): SparkConf
       = LocalSparkConf.newConf(addOn)

  protected def dirCleanup(): Unit = {
    if (dirList.nonEmpty) {
      dirList.foreach(FileCleaner.deletePath)
      dirList.clear()
    }
  }

  protected def baseCleanup(): Unit = {
    try {
      val scL = this.sc
      if (scL != null && !scL.isStopped) {
        val snc = this.snc
        snc.catalog.getTables(None).foreach {
          case (tableName, false) =>
            snc.dropExternalTable(tableName, ifExists = true)
          case (tableName, true) =>
            if (tableName.indexOf("_sampled") != -1) {
              snc.dropSampleTable(tableName, ifExists = true)
            } else {
              snc.dropTempTable(tableName, ifExists = true)
            }
        }
      }
    } finally {
      dirCleanup()
    }
  }

  override def beforeAll(): Unit = {
    baseCleanup()
  }

  override def afterAll(): Unit = {
    baseCleanup()
  }

  def stopAll(): Unit = {
    // GemFireXD stop for local mode is now done by SnappyContext.stop()
    println(" Stopping spark context = " + SnappyContext.globalSparkContext)
    SnappyContext.stop()
  }

  def createDir(fileName: String): String = {
    val f = new File(fileName)
    f.mkdir()
    f.deleteOnExit()
    dirList += fileName
    fileName
  }
}
