/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package io.snappydata.hydra.clusterUpgrade

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkConf, SparkContext}


object ClusterUpgradeSparkJob {
  // scalastyle:off println
  def main(args: Array[String]) {
    println(" Inside ClusterUpgradeSparkJob")
    val conf = new SparkConf().
        setAppName("ClusterUpgradeSparkJob Application")
    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)
    val queryFile: String = args(0)
    val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
    // scalastyle:off println
    val pw = new PrintWriter(new FileOutputStream(new File("ClusterUpgradeSparkJob.log"),
      true));
    runQuery(snc, queryArray)

    def runQuery(snc: SnappyContext, queryArray: Array[String]): Unit = {
      pw.println("Inside runQuery")
      for (j <- 0 to queryArray.length - 1) {
        val query = queryArray(j);
        val rs = snc.sql (query)
        pw.print("SP:Out put is " + rs.show())
      }
    }
    pw.close()
  }
}