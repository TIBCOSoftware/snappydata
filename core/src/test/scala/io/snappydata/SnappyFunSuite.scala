/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata

import java.io.File

import scala.collection.mutable.ArrayBuffer

import io.snappydata.core.{FileCleaner, LocalSparkConf}
import io.snappydata.test.dunit.DistributedTestBase
import io.snappydata.test.dunit.DistributedTestBase.{InitializeRun, WaitCriterion}
import io.snappydata.util.TestUtils

// scalastyle:off
import org.scalatest.{BeforeAndAfterAll, FunSuite, Outcome, Retries}

// scalastyle:on

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Base abstract class for all SnappyData tests similar to SparkFunSuite.
 */
abstract class SnappyFunSuite
    extends FunSuite // scalastyle:ignore
    with BeforeAndAfterAll
    with Serializable
    with Logging with Retries {

  InitializeRun.setUp()

  protected var testName: String = _
  protected val dirList = ArrayBuffer[String]()

  protected def sc: SparkContext = {
    val ctx = SnappyContext.globalSparkContext
    if (ctx != null && !ctx.isStopped) {
      ctx
    } else {
      cachedContext = null
      new SparkContext(newSparkConf())
    }
  }

  protected def sc(addOn: (SparkConf) => SparkConf): SparkContext = {
    val ctx = SnappyContext.globalSparkContext
    if (ctx != null && !ctx.isStopped) {
      ctx
    }
    else {
      cachedContext = null
      new SparkContext(newSparkConf(addOn))
    }
  }

  protected def scWithConf(addOn: (SparkConf) => SparkConf): SparkContext = {
    new SparkContext(newSparkConf(addOn))
  }

  @transient private var cachedContext: SnappyContext = _

  def getOrCreate(sc: SparkContext): SnappyContext = {
    val gnc = cachedContext
    if (gnc != null) gnc
    else synchronized {
      val gnc = cachedContext
      if (gnc != null) gnc
      else {
        cachedContext = SnappyContext(sc)
        cachedContext
      }
    }
  }

  protected def snc: SnappyContext = getOrCreate(sc)

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
      if (isRetryable(test)) withRetry {
        super.withFixture(test)
      } else super.withFixture(test)
    } finally {
      logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }

  def deleteDir(dir: String): Boolean = {
    FileCleaner.deletePath(dir)
  }

  protected def newSparkConf(addOn: SparkConf => SparkConf = null): SparkConf =
    LocalSparkConf.newConf(addOn)

  protected def dirCleanup(): Unit = {
    if (dirList.nonEmpty) {
      dirList.foreach(FileCleaner.deletePath)
      dirList.clear()
    }
  }

  protected def baseCleanup(clearStoreToBlockMap: Boolean = true): Unit = {
    try {
      TestUtils.dropAllTables(this.snc)
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

  /**
   * Wait until given criterion is met
   *
   * @param check          Function criterion to wait on
   * @param ms             total time to wait, in milliseconds
   * @param interval       pause interval between waits
   * @param throwOnTimeout if false, don't generate an error
   */
  def waitForCriterion(check: => Boolean, desc: String, ms: Long,
      interval: Long, throwOnTimeout: Boolean): Unit = {
    val criterion = new WaitCriterion {

      override def done: Boolean = {
        check
      }

      override def description() = desc
    }
    DistributedTestBase.waitForCriterion(criterion, ms, interval,
      throwOnTimeout)
  }

  def stopAll(): Unit = {
    val sparkContext = SnappyContext.globalSparkContext
    logInfo("Stopping spark context = " + sparkContext)
    if (sparkContext != null) sparkContext.stop()
    // GemFireXD stop for local mode is now done by SnappyContext.stop()
    cachedContext = null
  }

  def createDir(fileName: String): String = {
    val f = new File(fileName)
    f.mkdir()
    f.deleteOnExit()
    dirList += fileName
    fileName
  }

  protected def logStdOut(msg: String): Unit = {
    // scalastyle:off println
    println(msg)
    // scalastyle:on println
  }
}
