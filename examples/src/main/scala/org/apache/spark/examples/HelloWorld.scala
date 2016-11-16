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

package org.apache.spark.examples

import com.typesafe.config.{ConfigFactory, Config}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappyContext, SnappySQLJob}


object HelloWorld extends SnappySQLJob {

  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(sc: SnappyContext, jobConfig: Config): Any = "Hello Snappy Examples"

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName("LongPiJob")
    val sc = new SparkContext(conf)
    val snc = SnappyContext(sc)
    val config = ConfigFactory.parseString("")
    val results = runSnappyJob(snc, config)
    println("Result is " + results)
  }
}




