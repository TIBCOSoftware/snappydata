/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package io.snappydata.cluster

import java.io.{File, FileFilter}
import java.net.InetAddress
import java.nio.file.{Files, Paths}
import java.sql.{Connection, Statement}
import java.util.Properties

import scala.sys.process._

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.Constant
import io.snappydata.cluster.SplitClusterDUnitTest.stopSpark
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion
import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase, Host, SerializableRunnable, VM}
import io.snappydata.util.TestUtils
import org.apache.commons.io.FileUtils

import org.apache.spark.sql.hive.HiveClientUtil
import org.apache.spark.{TestPackageUtils}
import org.apache.spark.sql.{SnappyContext, SnappySession}


class RecoveryModeCatalogDUnitRecoveryTest(s: String) extends RecoveryModeDUnitTestBase(s)
    with SplitClusterDUnitTestBase
    with Serializable {


  private val jobConfigFile = s"$snappyProductDir/conf/job.config"

  def getJobJar(className: String, packageStr: String = ""): String = {
    val dir = new File(s"$snappyProductDir/../../../cluster/build-artifacts/scala-2.11/classes/"
        + s"scala/test/$packageStr")
    assert(dir.exists() && dir.isDirectory, s"snappy-cluster scala tests not compiled. Directory " +
        s"not found: $dir")
    val jar = TestPackageUtils.createJarFile(dir.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        pathname.getName.contains("RecoverModeJob")
      }
    }).toList, Some(packageStr))
    assert(!jar.isEmpty, s"No class files found for RecoverModeJob")
    jar
  }

  private def buildJobBaseStr(packageStr: String, className: String): String = {
    s"$snappyProductDir/bin/snappy-job.sh submit --app-name $className" +
        s" --class $packageStr.$className" +
        s" --app-jar ${getJobJar(className, packageStr.replaceAll("\\.", "/") + "/")}" +
        s" --passfile $jobConfigFile"
  }


  def submitAndVerifyJob(jobBaseStr: String, jobCmdAffix: String): Unit = {
    // Create config file with credentials
    writeToFile(s"-u $jdbcUser1:$jdbcUser1", jobConfigFile)

    val job = s"$jobBaseStr $jobCmdAffix"
      logInfo(s"Submitting job $job")
    val consoleLog = job.!!
    logInfo("consoleLog in submitAndVerifyJob" + consoleLog)
    // scalastyle:off println
    println("consoleLog in submitAndVerifyJob" + consoleLog)
    // scalastyle:on println

    val jobId = getJobId(consoleLog)
    assert(consoleLog.contains("STARTED"), "Job not started")

    val wc = getWaitCriterion(jobId)
    DistributedTestBase.waitForCriterion(wc, 100000, 1000, true)
  }

  val opCode = "op.code"
  val outputFile = "output.file"

  private def getJobId(str: String): String = {
    logInfo("getJobId str " + str)
    val idx = str.indexOf("jobId")
    logInfo("getJobId idx " + idx.toString)

    str.substring(idx + 9, idx + 45)
  }

  private def getWaitCriterion(jobId: String): WaitCriterion = {
    new WaitCriterion {
      var consoleLog = ""
      override def done() = {
        consoleLog = (s"$snappyProductDir/bin/snappy-job.sh status --job-id $jobId " +
            s" --passfile $jobConfigFile").!!
        if (consoleLog.contains("FINISHED")) logInfo(s"Job $jobId completed. $consoleLog")
        consoleLog.contains("FINISHED")
      }
      override def description() = {
        logInfo(consoleLog)
        s"Job $jobId did not complete in time."
      }
    }
  }

  def testSnappyJob(): Unit = {
    val jobBaseStr = buildJobBaseStr("io.snappydata.cluster", "RecoverModeJob")
    submitAndVerifyJob(jobBaseStr, s" --conf $opCode=sqlOps --conf $outputFile=SnappyValidJob.out")
  }


  def testSnappyJDBC(): Unit = {
    val props = new Properties()

    // scalastyle:off println
    logInfo(" dummy test called")
    println("dummy test called...")
    println("testdummy : snappy cluster mode is : "
        + SnappyContext.getClusterMode(snc.sparkContext))
    // scalastyle:on println

    props.setProperty(Attribute.USERNAME_ATTR, jdbcUser1)
    props.setProperty(Attribute.PASSWORD_ATTR, jdbcUser1)
  val conn = RecoveryModeDUnitTestBase.getConnection(locatorClientPort, props)

    // scalastyle:off println
    println("conn.getCatalog === " + conn.getMetaData.getDriverName)
//
//    println(" -- *** " + HiveClientUtil.
//        getOrCreateExternalCatalog(snc.sparkContext, snc.sparkContext.getConf).getAllTables())
//     scalastyle:on println


        user1Conn = getConn(jdbcUser1, true)
        var stmt = user1Conn.createStatement()


        try {
          val rs = stmt.executeQuery("show tables in app")
          while (rs.next()) {
            val c2 = rs.getString("tableName")
            // scalastyle:off println
            println("tableNames:\n" + c2)
            // scalastyle:on println
          }
          rs.close()
        } finally {
          stmt.close()
          user1Conn.close()
        }
  }

}

