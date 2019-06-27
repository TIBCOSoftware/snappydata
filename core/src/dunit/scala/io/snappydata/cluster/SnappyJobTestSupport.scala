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

import io.snappydata.test.dunit.DistributedTestBase
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion

import org.apache.spark.{Logging, TestPackageUtils}
import scala.sys.process._

import org.apache.commons.lang.StringUtils

trait SnappyJobTestSupport extends Logging {

  val snappyProductDir: String

  val jobConfigFile : String = null

  def submitAndWaitForCompletion(classFullName: String, jobCmdAffix: String = "",
      waitTimeMillis: Int = 60000): Unit = {
    val consoleLog: String = submitJob(classFullName, jobCmdAffix)
    logInfo(consoleLog)
    val jobId = getJobId(consoleLog)
    assert(consoleLog.contains("STARTED"), "Job not started")

    val wc = getWaitCriterion(jobId)
    DistributedTestBase.waitForCriterion(wc, waitTimeMillis, 1000, true)
  }

  def submitJob(classFullName: String, jobCmdAffix: String): String = {
    val className = StringUtils.substringAfterLast(classFullName, ".")
    val packageStr = StringUtils.substringBeforeLast(classFullName, ".")
    val job = s"${buildJobSubmissionCommand(packageStr, className)} $jobCmdAffix"
    logInfo(s"Submitting job $job")
    job.!!
  }

  private def buildJobSubmissionCommand(packageStr: String, className: String): String = {
    val jobSubmissionCommand = s"$snappyProductDir/bin/snappy-job.sh submit --app-name $className" +
        s" --class $packageStr.$className" +
        s" --app-jar ${getJobJar(className, packageStr.replaceAll("\\.", "/") + "/")}"
    if(jobConfigFile != null){
      jobSubmissionCommand + s" --passfile $jobConfigFile"
    } else jobSubmissionCommand
  }

  private def getJobJar(className: String, packageStr: String = ""): String = {
    val dir = new File(s"$snappyProductDir/../../../cluster/build-artifacts/scala-2.11/classes/"
        + s"scala/test/$packageStr")
    assert(dir.exists() && dir.isDirectory, s"snappy-cluster scala tests not compiled. Directory " +
        s"not found: $dir")
    val jar = TestPackageUtils.createJarFile(dir.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        true
      }
    }).toList, Some(packageStr))
    assert(!jar.isEmpty, s"No class files found for Job")
    jar
  }

  private def getJobId(str: String): String = {
    val idx = str.indexOf("jobId")
    str.substring(idx + 9, idx + 45)
  }


  private def getWaitCriterion(jobId: String): WaitCriterion = {
    new WaitCriterion {
      var consoleLog = ""
      override def done() = {
        val jobStatusCommand = s"$snappyProductDir/bin/snappy-job.sh status --job-id $jobId"
        consoleLog = if (jobConfigFile != null){
          (jobStatusCommand + s" --passfile $jobConfigFile").!!
        } else {
          jobStatusCommand.!!
        }
        if (consoleLog.contains("FINISHED")) logInfo(s"Job $jobId completed. $consoleLog")
        else if (!consoleLog.contains("RUNNING")) {
          throw new Exception("Job failied with result:" + consoleLog)
        }
        consoleLog.contains("FINISHED")
      }
      override def description() = {
        logInfo(consoleLog)
        s"Job $jobId did not complete in time."
      }
    }
  }
}
