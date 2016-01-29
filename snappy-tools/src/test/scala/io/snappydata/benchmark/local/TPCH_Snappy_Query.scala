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

package io.snappydata.benchmark.local

import com.typesafe.config.Config
import io.snappydata.benchmark.TPCH_Snappy
import spark.jobserver.{SparkJobValid, SparkJobValidation}

import org.apache.spark.sql.SnappySQLJob

/**
  * Created by kishor on 28/1/16.
  */
object TPCH_Snappy_Query extends SnappySQLJob{



   override def runJob(snc: C, jobConfig: Config): Any = {
     val isResultCollection = false
     val isSnappy = true

     val usingOptionString = s"""
           USING row
           OPTIONS ()"""



     for(i <- 1 to 3) {
       println("**************Iteration $i ***************************")
       TPCH_Snappy.execute("q1", snc, isResultCollection, isSnappy, i)
       //TPCH_Snappy.execute("q2", snc,isResultCollection, isSnappy) //working in local mode but not in cluster mode
       TPCH_Snappy.execute("q3", snc, isResultCollection, isSnappy, i)
       TPCH_Snappy.execute("q4", snc, isResultCollection, isSnappy, i)
       TPCH_Snappy.execute("q5", snc, isResultCollection, isSnappy, i)
       TPCH_Snappy.execute("q6", snc, isResultCollection, isSnappy, i)
       TPCH_Snappy.execute("q7", snc, isResultCollection, isSnappy, i)
       TPCH_Snappy.execute("q8", snc, isResultCollection, isSnappy, i)
       TPCH_Snappy.execute("q9", snc, isResultCollection, isSnappy, i)
       TPCH_Snappy.execute("q10", snc, isResultCollection, isSnappy, i)
       TPCH_Snappy.execute("q11", snc, isResultCollection, isSnappy, i)
       TPCH_Snappy.execute("q12", snc, isResultCollection, isSnappy, i)
       TPCH_Snappy.execute("q13", snc, isResultCollection, isSnappy, i)
       TPCH_Snappy.execute("q14", snc, isResultCollection, isSnappy, i)
       TPCH_Snappy.execute("q15", snc, isResultCollection, isSnappy, i)
       TPCH_Snappy.execute("q16", snc, isResultCollection, isSnappy, i)
       TPCH_Snappy.execute("q17", snc, isResultCollection, isSnappy, i)
       TPCH_Snappy.execute("q18", snc, isResultCollection, isSnappy, i)
       //TPCH_Snappy.execute("q19", snc,isResultCollection, isSnappy, i) //not working in local mode hence not executing it for cluster mode too
       TPCH_Snappy.execute("q20", snc, isResultCollection, isSnappy, i)
       //TPCH_Snappy.execute("q21", snc,isResultCollection, isSnappy, i) //not working in local mode hence not executing it for cluster mode too
       TPCH_Snappy.execute("q22", snc, isResultCollection, isSnappy, i)
       println("---------------------------------------------------------------------------------")
     }

   }

   override def validate(sc: C, config: Config): SparkJobValidation = {
     SparkJobValid
   }
 }
