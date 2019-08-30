
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
package io.snappydata.hydra.testEndToEndValidation

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object ValidateFailureScenarioApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().
        setAppName("TestHangInSparkApp_" + System.currentTimeMillis())
    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)
    val pw: PrintWriter
    = new PrintWriter(new FileOutputStream(new File("testAppFailure.out")), true)
    // scalastyle:off println
    pw.println("Throwing test exception...")
    pw.flush()
    throw new Exception("Throwing test exception...")
  }
}
