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

import java.io.{File, FileOutputStream, PrintWriter}

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
      "using jar '/home/cbhatt/TestWork/out/artifacts/TestWork_jar/MeanMarkUDAF.jar'"

    val createCleverUDAF = "create function cleverstudent_udaf " +
      "as io.snappydata.hydra.CleverStudent " +
      "returns double using jar " +
      "'/home/cbhatt/TestWork/out/artifacts/TestWork_jar/cleverStudent.jar'"

    val createDullUDAF = "create function dullstudent_udaf as " +
      "io.snappydata.hydra.DullStudent " +
      "returns double using jar " +
      "'/home/cbhatt/TestWork/out/artifacts/TestWork_jar/dullStudent.jar'"


    val createUDAFWithDifferentFunctionality = "create function mean_udaf " +
      "as io.snappydata.hydra.CleverStudent returns double " +
      "using jar '/home/cbhatt/TestWork/MeanMarkUDAF.jar'"

    val createUDAFWrongJarPath = "create function mean_udaf as io.snappydata.hydra.MeanMarks " +
      "returns double " +
      "using jar '/home/cbhatt/TestWork/jars/MeanMarkUDAF.jar'"

    val createUDAFWrongQualifiedClass = "create function mean_udaf " +
      "as io.snappydata.udaftest.AvgMarks " +
      "returns double " +
      "using jar '/home/cbhatt/TestWork/out/artifacts/TestWork_jar/MeanMarkUDAF.jar'"

    val createUDAFWrongReturnType = "create function mean_udaf as io.snappydata.hydra.MeanMarks " +
      "returns Boolean " +
      "using jar '/home/cbhatt/TestWork/out/artifacts/TestWork_jar/MeanMarkUDAF.jar'"

    val dropUDAF = "drop function if exists mean_udaf"

    val createStagingStudent = "create external table if not exists staging_student" +
      "(studentid int,name string,class int," +
      "maths double,english double,physics double,socialst double,year int,total double) " +
      "using csv options(path '/home/cbhatt/UDAF_Data_1')"

    val dropStagingStudent = "drop table if exists stagingStudent"

    val createStudent = "create table if not exists " +
      "student using column as select * from staging_student"

    val dropStudent = "drop table if exists Student"

    val outputFile = "ValidateUDAF_" +  System.currentTimeMillis()
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), true))
    pw.println("UDAF validation started....")

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
      "cleverstudent_udaf(maths), test_udaf(maths,0,0) from student group by class order by class"


    /**
      * Create UDAF, test it, drop the function
      *  again create the same UDAF, test it and drop it.
      *  Result should be the same.
      */
    pw.println("Create UDAF, test it, drop the function and repeat it.")
    snc.sql("drop function if exists cleverstudent_udaf")
    snc.sql("drop function if exists dullstudent_udaf")
    snc.sql(dropUDAF)
    snc.sql(dropStagingStudent)
    snc.sql(dropStudent)
    snc.sql(createUDAF)
    snc.sql(createStagingStudent)
    snc.sql(createStudent)
    val sncDF1 = snc.sql(query_1_UDAF)
    val sncDF2 = snc.sql(query_1_Snappy)
    validateResultSet(snc, sncDF1, sncDF2, pw, query_1_UDAF)
    snc.sql(dropUDAF)
    snc.sql(createUDAF)
    val sncDF3 = snc.sql(query_2_UDAF)
    val sncDF4 = snc.sql(query_2_Snappy)
    validateResultSet(snc, sncDF3, sncDF4, pw, query_2_UDAF)
    snc.sql(dropUDAF)

    /**
      * Create UDAF with wrong jar path.
      *  Result : An Exception
      *  Drop the UDAF.
      */
    try {
      pw.println("Create UDAF with wrong jar path.")
      snc.sql(createUDAFWrongJarPath)
    } catch {
      case e : Exception => {
        pw.println("Exception when wrong jar path given -> " + e.getMessage)
        pw.println("*     *     *     *     *     *     *     *     *     *")
        pw.flush()
      }
    }
    snc.sql(dropUDAF)

    /**
      * Create UDAF with wrong qualified class name.
      *  test the UDAF results into an Exception.
      *  Drop the UDAF.
       */
    try {
      pw.println("Create UDAF with wrong qualified class name.")
      snc.sql(createUDAFWrongQualifiedClass)
      snc.sql(query_1_UDAF)
    } catch {
      case e : Exception => {
        pw.println("Exception when wrong qualified class path given -> " + e.getMessage)
        pw.println("*     *     *     *     *     *     *     *     *     *")
        pw.flush()
      }
    }
    snc.sql(dropUDAF)

    /**
      * Create the UDAF, test it and then drop the UDAF.
      * Now without creating it again test the UDAF
      * results into an Exception.
      */
    try {
      pw.println("Create UDAF, test and drop it and without creating UDAF test it.")
      snc.sql(createUDAF)
      val sncDF1 = snc.sql(query_1_UDAF)
      val sncDF2 = snc.sql(query_1_Snappy)
      validateResultSet(snc, sncDF1, sncDF2, pw, query_1_UDAF)
      snc.sql(dropUDAF)
      val sncDF3 = snc.sql(query_2_UDAF)
    } catch {
      case e : Exception => {
        pw.println("Without creating the UDAF, call the UDAF produce -> " + e.getMessage)
        pw.println("*     *     *     *     *     *     *     *     *     *")
        pw.flush()
      }
    }

    /**
      * Create the UDAF with wrong return type.
      *  Testing of UDAF should results into an Exception.
      *  Presently exception is not thrown raise the ticket SNAP - 3170.
      *  drop the UDAF.
      */
    try {
      pw.println("Create UDAF with wrong return data type.")
      snc.sql(createUDAFWrongReturnType)
      val sncDF1 = snc.sql(query_1_UDAF)
      val sncDF2 = snc.sql(query_1_Snappy)
      validateResultSet(snc, sncDF1, sncDF2, pw, query_1_UDAF)
     } catch {
      case e : Exception => {
        pw.println("When wrong return type given to function produce -> " + e.getMessage)
        pw.println("*     *     *     *     *     *     *     *     *     *")
        pw.flush()
      }
    }
    snc.sql(dropUDAF)

    /**
      * Create the UDAF, test it and drop it.
      *  Create the UDAF with the same jar name but functionality of UDAF  will be different.
      *  then test the UDAF no exception should occur then drop the UDAF.
      *  Test that jar  gets undeployed cleanly from cluster.
      */
    try {
      pw.println("Test UDAF : jar gets undeployed clearnly from cluster.")
      snc.sql(createUDAF)
      val sncDF1 = snc.sql(query_1_UDAF)
      val sncDF2 = snc.sql(query_1_Snappy)
      validateResultSet(snc, sncDF1, sncDF2, pw, query_1_UDAF)
      snc.sql(dropUDAF)
      snc.sql(createUDAFWithDifferentFunctionality)
      val sncDF3 = snc.sql(query_3_UDAF)
      val sncDF4 = snc.sql(query_3_Snappy)
      validateResultSet(snc, sncDF3, sncDF4, pw, query_3_UDAF)
    } catch {
      case e : Exception => {
        pw.println("Undepoly jar cleanly from cluster produce -> " + e.getMessage)
        pw.println("*     *     *     *     *     *     *     *     *     *")
        pw.flush()
      }
   }
    snc.sql(dropUDAF)

    try {
      pw.println("Testing : Multiple UDF in select statement")
      snc.sql(dropUDAF)
      snc.sql("drop function if exists cleverstudent_udaf")
      snc.sql("drop function if exists dullstudent_udaf")
      snc.sql(createUDAF)
      snc.sql(createCleverUDAF)
      snc.sql(createDullUDAF)
      val sncDF1 = snc.sql(query_mulitple_UDAF)
      sncDF1.show()
      pw.println("Muliple UDF testing OK")
    } catch {
      case e : Exception => {
        pw.println("Exception in multiple UDF -> " + e.getMessage)
        pw.println("*     *     *     *     *     *     *     *     *     *")
        pw.flush()
      }
    }

    snc.sql(dropUDAF)
    snc.sql("drop function if exists cleverstudent_udaf")
    snc.sql("drop function if exists dullstudent_udaf")

    snc.sql(dropStagingStudent)
    snc.sql(dropStudent)
    pw.println("UDAF validation ends successfully...")
    println("UDAF validation ends successfully...")
    pw.flush()
    pw.close()
  }

  def validateResultSet(snc : SnappyContext, sncDF1 : DataFrame,
                        sncDF2 : DataFrame, pw : PrintWriter, msg : String) : Unit = {
    var isDiff1 = false
    var isDiff2 = false
    val diff1 = sncDF1.except(sncDF2)
    val diff2 = sncDF2.except(sncDF1)
    sncDF1.show()
    sncDF2.show()
    if(diff1.count() > 0) {
      println("Difference found in sncDF2")
      isDiff1 = true
    }
    if(diff2.count() > 0) {
      println("Difference found in sncDF1")
      isDiff2 = true
    }
    if(isDiff1 == false && isDiff2 == false) {
      pw.println(msg + " is passed.")
    }
    pw.println("*     *     *     *     *     *     *     *     *     *")
    pw.flush()
  }
}
