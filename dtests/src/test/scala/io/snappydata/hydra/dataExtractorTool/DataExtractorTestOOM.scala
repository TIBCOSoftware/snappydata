/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package io.snappydata.hydra.dataExtractorTool

import com.typesafe.config.Config
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession, _}


object DataExtractorTestOOM extends SnappySQLJob {
  // scalastyle:off println
  println(" Inside arrayCreation")

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    // scalastyle:off println
      try {
      Utils.mapExecutors[Unit](snSession.sqlContext.sparkContext, () => {
        for ( i <- 0 to 100000) {
          System.out.println("creating array " + i)
          val arr1 = new Array[Byte](1073741824);
        }
        Iterator.empty
      })
    } finally {
      // StratifiedSampler.removeSampler(sampleTable, markFlushed = false)
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}

