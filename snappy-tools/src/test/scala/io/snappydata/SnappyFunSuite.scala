package io.snappydata

import java.io.{File, FilenameFilter}

import org.scalatest.{FunSuite, Outcome}

import org.apache.spark.Logging

/**
  * Base abstract class for all snappydata tests
  * similar to SparkFunSuite.
  *
  * Created by soubhikc on 6/10/15.
  */
private[snappydata] abstract class SnappyFunSuite extends FunSuite // scalastyle:ignore
with Logging {

  var dirList = Array[File]()

  @volatile private[this] var _testName: String = _

  protected def testName = _testName

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
    _testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("io.snappydata", "i.sd")
    try {
      logInfo(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      val outcome: Outcome = test()
      if (outcome.isSucceeded) {
        cleanup()
      }
      outcome
    } finally {
      logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }

  def deleteDir(dir: File): Boolean = {
    val clazz = Class.forName("com.pivotal.gemfirexd.TestUtil") //scalastyle:ignore
    val res = clazz.getMethod("deleteDir", classOf[File])
        .invoke(null, dir)
    res.toString.toBoolean
  }

  def cleanup(): Unit = {
    val clearList = dirList
    dirList = Array[File]()
    clearList.foreach(deleteDir)
    deleteDir(new File("metastore_db"))
    deleteDir(new File("datadictionary"))
    val filter = new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.startsWith("BACKUPGFXD-DEFAULT-DISKSTORE") ||
            (name.startsWith("locator") && name.endsWith(".dat"))
      }
    }

    for (f <- new File(".").listFiles(filter)) {
      deleteDir(f)
    }
  }

  def createDir(fileName: String): String = {
    val f = new File(fileName)
    f.mkdir()
    dirList :+= f
    fileName
  }
}
