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
package io.snappydata.hydra.udaf

import com.typesafe.config.Config
import org.apache.spark.sql._

class UDAFValidation extends SnappySQLJob {
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    // scalastyle:off println
    println("UDAF validation started....")
    val snc : SnappyContext = snappySession.sqlContext
    val createUDAF = "create function mean_udaf as io.snappydata.hydra.MeanMarks " +
      "returns double " +
      "using jar '/home/cbhatt/TestWork/out/artifacts/TestWork_jar/TestWork.jar'"
    val dropUDAF = "drop function if exists mean_udaf"
    val createStagingStudent = "create external table if not exists staging_student" +
      "(studentid int,name string,class int," +
      "maths double,english double,physics double,socialst double,year int,total double) " +
      "using csv options(path '/home/cbhatt/UDAF_Data_1')"
    val dropStagingStudent = "drop table if exists stagingStudent"
    val createStudent = "create table if not exists " +
      "student using column as select * from staging_student"
    val dropStudent = "drop table if exists Student"
    val query_1_UDAF = "select class,mean_udaf(maths,class) from  student " +
      "group by class order by class"
    val query_1_Snappy = "select class,avg(maths) from student group by class order by class"
    val query_2_UDAF = "select class,mean_udaf(physics,class) from  student " +
      "group by class order by class"
    val query_2_Snappy = "select class,avg(physics) from student group by class order by class"

    snc.sql(createUDAF)
    snc.sql(dropStagingStudent)
    snc.sql(dropStudent)
    snc.sql(createStagingStudent)
    snc.sql(createStudent)
    val sncDF1 = snc.sql(query_1_UDAF)
    val sncDF2 = snc.sql(query_1_Snappy)
    validateResultSet(snc, sncDF1, sncDF2)
    snc.sql(dropUDAF)
    snc.sql(createUDAF)
    val sncDF3 = snc.sql(query_2_UDAF)
    val sncDF4 = snc.sql(query_2_Snappy)
    validateResultSet(snc, sncDF3, sncDF4)
    snc.sql(dropUDAF)
    snc.sql(dropStagingStudent)
    snc.sql(dropStudent)
    println("UDAF validation ends successfully...")
  }

  def validateResultSet(snc : SnappyContext, sncDF1 : DataFrame, sncDF2 : DataFrame) : Unit = {
    val diff1 = sncDF1.except(sncDF2)
    val diff2 = sncDF2.except(sncDF1)
    println(sncDF1.show())
    println(sncDF2.show())
    if(diff1.count() > 0) {
      println("Difference found in sncDF2")
    }
    if(diff2.count() > 0) {
      println("Difference found in sncDF1")
    }
  }
}
