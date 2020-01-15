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

object UDAFUtils {
  val createUDAF = "create function mean_udaf as io.snappydata.hydra.MeanMarks " +
    "returns double " +
    "using jar '/export/shared/QA_DATA/UDAF/jars/MeanMarkUDAF.jar'"

  val createCleverUDAF = "create function cleverstudent_udaf " +
    "as io.snappydata.hydra.CleverStudent " +
    "returns double using jar " +
    "'/export/shared/QA_DATA/UDAF/jars/cleverStudent.jar'"

  val createDullUDAF = "create function dullstudent_udaf as " +
    "io.snappydata.hydra.DullStudent " +
    "returns double using jar " +
    "'/export/shared/QA_DATA/UDAF/jars/dullStudent.jar'"

  val createUDAFWithDifferentFunctionality = "create function mean_udaf " +
    "as io.snappydata.hydra.CleverStudent returns double " +
    "using jar '/export/shared/QA_DATA/UDAF/jars/MeanMarkDiffFunc/MeanMarkUDAF.jar'"

  val createUDAFWrongJarPath = "create function mean_udaf as io.snappydata.hydra.MeanMarks " +
    "returns double " +
    "using jar '/home/cbhatt/TestWork/jars/MeanMarkUDAF.jar'"

  val createUDAFWrongQualifiedClass = "create function mean_udaf " +
    "as io.snappydata.udaftest.AvgMarks " +
    "returns double " +
    "using jar '/export/shared/QA_DATA/UDAF/jars/MeanMarkUDAF.jar'"

  val createUDAFWrongReturnType = "create function mean_udaf as io.snappydata.hydra.MeanMarks " +
    "returns Boolean " +
    "using jar '/export/shared/QA_DATA/UDAF/jars/MeanMarkUDAF.jar'"

  val dropUDAF = "drop function if exists mean_udaf"
  val dropCleverUDAF = "drop function if exists cleverstudent_udaf"
  val dropDullUDAF = "drop function if exists dullstudent_udaf";

  val createStagingStudent = "create external table if not exists staging_student" +
    "(studentid int,name string,class int," +
    "maths double,english double,physics double,socialst double,year int,total double) " +
    "using csv options(path '/export/shared/QA_DATA/UDAF/data/UDAF_Data')"

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

  val query_3_UDAF = "select class,mean_udaf(maths) from student group by class order by class"
  val query_3_Snappy = "select class,count(maths) from student" +
    " where maths >= 95.0 group by class order by class"

  val query_mulitple_UDAF = "select class, mean_udaf(maths,class), " +
    "cleverstudent_udaf(maths), dullstudent_udaf(maths,0,0) " +
    "from student group by class order by class"
}
