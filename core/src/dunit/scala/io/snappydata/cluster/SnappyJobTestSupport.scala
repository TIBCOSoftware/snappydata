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

/**
 * A helper trait containing functions for managing snappy jobs.
 */
trait SnappyJobTestSupport extends Logging {

  val snappyProductDir: String

  /**
   * For secure cluster scenarios this value should be extended with config file containing
   * credentials.
   */
  val jobConfigFile : String = null

  private lazy val snappyJobScript = s"$snappyProductDir/bin/snappy-job.sh"

  /**
   * Submits a snappy job and wait for time specified by `waitTimeMillis` parameter before failing.
   *
   * @param classFullName   fully qualified name of the job class
   * @param jobCmdAffix     additional configs needs to be passed while submitting the job
   * @param waitTimeMillis  expected job execution time in miliseconds. If the execution time
   *                        exceeds specified time, the method will throw exception.
   */
  def submitAndWaitForCompletion(classFullName: String, jobCmdAffix: String = "",
      waitTimeMillis: Int = 60000): Unit = {
    val consoleLog: String = submitJob(classFullName, jobCmdAffix)
    logInfo(consoleLog)
    val jobId = getJobId(consoleLog)
    assert(consoleLog.contains("STARTED"), "Job not started")

    waitForJobCompletion(waitTimeMillis, jobId)
  }

  /**
   * Waits for job completion for the amount of time specified by `waitTimeMillis`.
   * Returns successfully if the job reaches "FINISHED" state within time specified by
   * `waitTimeMillis`.
   * If job goes to any state other than "RUNNING" and "FINISHED", it throws and exception
   * containing job status call response.
   *
   * @param waitTimeMillis time to wait in millis.
   * @param jobId          id of the job to wait for
   */

  def waitForJobCompletion(waitTimeMillis: Int = 60000, jobId: String): Unit = {
    val wc = getWaitCriterion(jobId)
    DistributedTestBase.waitForCriterion(wc, waitTimeMillis, 1000, true)
  }

  /**
   * Submits a snappy job
   *
   * @param classFullName fully qualified name of the job class
   * @param jobCmdAffix additional configs needs to be passed while submitting the job
   * @return job submission result
   */
  def submitJob(classFullName: String, jobCmdAffix: String): String = {
    val className = StringUtils.substringAfterLast(classFullName, ".")
    val packageStr = StringUtils.substringBeforeLast(classFullName, ".")
    val job = s"${buildJobSubmissionCommand(packageStr, className)} $jobCmdAffix"
    logInfo(s"Submitting job $job")
    job.!!
  }

  private def buildJobSubmissionCommand(packageStr: String, className: String): String = {
    val jobSubmissionCommand = s"$snappyJobScript submit --app-name $className" +
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
    val jar = TestPackageUtils.createJarFile(dir.listFiles, Some(packageStr))
    assert(!jar.isEmpty, s"No class files found for Job")
    jar
  }

  /**
   * Extracts job id from the job submission response
   * @param str job submission response
   * @return id of the job
   */
  def getJobId(str: String): String = {
    // todo: use some JSON parser to fetch job id instead of doing string processing
    val idx = str.indexOf("jobId")
    str.substring(idx + 9, idx + 45)
  }


  private def getWaitCriterion(jobId: String): WaitCriterion = {
    new WaitCriterion {
      var consoleLog = ""
      override def done() = {
        val jobStatusCommand = s"$snappyJobScript status --job-id $jobId"
        consoleLog = if (jobConfigFile != null){
          (jobStatusCommand + s" --passfile $jobConfigFile").!!
        } else {
          jobStatusCommand.!!
        }
        if (consoleLog.contains("FINISHED")) logInfo(s"Job $jobId completed. $consoleLog")
        else if (!consoleLog.contains("RUNNING")) {
          throw new Exception("Job failed with result:" + consoleLog)
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
