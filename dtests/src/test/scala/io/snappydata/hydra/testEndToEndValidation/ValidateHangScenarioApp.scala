
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
package io.snappydata.hydra.testEndToEndValidation

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object ValidateHangScenarioApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().
        setAppName("TestFailureInSparkApp_" + System.currentTimeMillis())
    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val outputFile = "testAppHang.out"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    // scalastyle:off println
    pw.println("Thread goes to sleep now...")
    pw.flush()
    Thread.sleep(300 * 1000)
    pw.println("Done sleeping")
    pw.flush()
  }
}
