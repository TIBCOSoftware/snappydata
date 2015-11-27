package io.snappydata

import java.io.File

import scala.collection.mutable.ArrayBuffer

import io.snappydata.core.{FileCleaner, LocalSparkConf}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Outcome}

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

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
    if (ctx != null) ctx
    else new SparkContext(newSparkConf())
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

  protected def newSparkConf(): SparkConf = LocalSparkConf.newConf()

  private def baseCleanup(): Unit = {
    try {
      val sc = this.sc
      if (sc != null && !sc.isStopped) {
        snc.catalog.getTables(None).foreach {
          case (tableName, false) =>
            snc.dropExternalTable(tableName, ifExists = true)
          case _ =>
        }
      }
      SnappyContext.stop()
    } finally {
      if (dirList.nonEmpty) {
        dirList.foreach(FileCleaner.deletePath)
        dirList.clear()
      }
    }
  }

  override def beforeAll(): Unit = {
    baseCleanup()
  }

  override def afterAll(): Unit = {
    baseCleanup()
  }

  def createDir(fileName: String): String = {
    val f = new File(fileName)
    f.mkdir()
    f.deleteOnExit()
    dirList += fileName
    fileName
  }
}
