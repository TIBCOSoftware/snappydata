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
package io.snappydata.hydra

import java.io.{File, FileOutputStream, PrintWriter, StringWriter}

import com.typesafe.config.Config
import io.snappydata.hydra.installJar.TestUtils
import org.apache.spark.sql._

class InstallJarTest extends SnappySQLJob {
  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    //DynamicJarLoadingTest.verify(snc, jobConfig.getString("classVersion"))
      val pw: PrintWriter = new PrintWriter(new FileOutputStream(new File(jobConfig.getString("logFileName"))), true)
      try {
        pw.println("****** DynamicJarLoadingJob started ******")
        val currentDirectory: String = new File(".").getCanonicalPath
        TestUtils.verify(snc, jobConfig.getString("classVersion"), pw)
        pw.println("****** DynamicJarLoadingJob finished ******")
        return String.format("See %s/" + jobConfig.getString("logFileName"), currentDirectory)
      }
      catch {
        case e: Exception => {
          val sw: StringWriter = new StringWriter
          val spw: PrintWriter = new PrintWriter(sw)
          spw.println("ERROR: failed with " + e)
          e.printStackTrace(spw)
          return spw.toString
        }
      } finally {
        if (pw != null) pw.close()
      }
    }

  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()
}
