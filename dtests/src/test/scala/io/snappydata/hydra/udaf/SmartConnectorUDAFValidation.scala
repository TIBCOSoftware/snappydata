/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SnappyContext, SparkSession}

object SmartConnectorUDAFValidation {
  def main(args: Array[String]): Unit = {
    //  scalastyle:off println
    println("Smart connector UDAF validation Job Started...")
    val connectionURL: String = args(args.length - 1)
    println("ConnectionURL : " + connectionURL)
    val conf: SparkConf = new SparkConf()
      .setAppName("UDAF Validation Job")
      .set("snappydata.connection", connectionURL)
    val sc: SparkContext = SparkContext.getOrCreate(conf)
    val snc: SnappyContext = SnappyContext(sc)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val outputFile = "ValidateUDAF_" + System.currentTimeMillis()
    val pw: PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), true))
    pw.println("Smart connector UDAF validation Job Started...")
    val printDFContent: Boolean = false
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()

    /**
      * Create UDAF, test it, drop the function
      * again create the same UDAF, test it and drop it.
      * Result should be the same.
      */
        pw.println("Create UDAF, test it, drop the function and repeat it.")
        snc.sql(UDAFUtils.dropCleverUDAF)
        snc.sql(UDAFUtils.dropDullUDAF)
        snc.sql(UDAFUtils.dropUDAF)
        snc.sql(UDAFUtils.dropStagingStudent)
        snc.sql(UDAFUtils.dropStudent)
        snc.sql(UDAFUtils.createUDAF)
        snc.sql(UDAFUtils.createStagingStudent)
        snc.sql(UDAFUtils.createStudent)
        val sncDF1 = snc.sql(UDAFUtils.query_1_UDAF)
        val sncDF2 = snc.sql(UDAFUtils.query_1_Snappy)
        validateResultSet(snc, sncDF1, sncDF2, pw, UDAFUtils.query_1_UDAF)
        snc.sql(UDAFUtils.dropUDAF)
        snc.sql(UDAFUtils.createUDAF)
        val sncDF3 = snc.sql(UDAFUtils.query_2_UDAF)
        val sncDF4 = snc.sql(UDAFUtils.query_2_Snappy)
        validateResultSet(snc, sncDF3, sncDF4, pw, UDAFUtils.query_2_UDAF)
        snc.sql(UDAFUtils.dropUDAF)

    /**
      * Create UDAF with wrong jar path.
      * Result : An Exception
      * Drop the UDAF.
      */
        try {
          pw.println("Create UDAF with wrong jar path.")
          snc.sql(UDAFUtils.createUDAFWrongJarPath)
        } catch {
          case e : Exception => {
            pw.println("Exception when wrong jar path given -> " + e.getMessage)
            pw.println("*     *     *     *     *     *     *     *     *     *")
            pw.flush()
          }
        }
        snc.sql(UDAFUtils.dropUDAF)

    /**
      * Create UDAF with wrong qualified class name.
      * test the UDAF results into an Exception.
      * Drop the UDAF.
      */
        try {
          pw.println("Create UDAF with wrong qualified class name.")
          snc.sql(UDAFUtils.createUDAFWrongQualifiedClass)
          snc.sql(UDAFUtils.query_1_UDAF)
        } catch {
          case e : Exception => {
            pw.println("Exception when wrong qualified class path given -> " + e.getMessage)
            pw.println("*     *     *     *     *     *     *     *     *     *")
            pw.flush()
          }
        }
        snc.sql(UDAFUtils.dropUDAF)

    /**
      * Create the UDAF, test it and then drop the UDAF.
      * Now without creating it again test the UDAF
      * results into an Exception.
      */
        try {
          pw.println("Create UDAF, test and drop it and without creating UDAF test it.")
          snc.sql(UDAFUtils.createUDAF)
          val sncDF1 = snc.sql(UDAFUtils.query_1_UDAF)
          val sncDF2 = snc.sql(UDAFUtils.query_1_Snappy)
          validateResultSet(snc, sncDF1, sncDF2, pw, UDAFUtils.query_1_UDAF)
          snc.sql(UDAFUtils.dropUDAF)
          val sncDF3 = snc.sql(UDAFUtils.query_2_UDAF)
        } catch {
          case e : Exception => {
            pw.println("Without creating the UDAF, call the UDAF produce -> " + e.getMessage)
            pw.println("*     *     *     *     *     *     *     *     *     *")
            pw.flush()
          }
        }

    /**
      * Create the UDAF with wrong return type.
      * Testing of UDAF should results into an Exception.
      * Presently exception is not thrown raise the ticket SNAP - 3170.
      * drop the UDAF.
      */
        try {
          pw.println("Create UDAF with wrong return data type.")
          snc.sql(UDAFUtils.createUDAFWrongReturnType)
          val sncDF1 = snc.sql(UDAFUtils.query_1_UDAF)
          val sncDF2 = snc.sql(UDAFUtils.query_1_Snappy)
          validateResultSet(snc, sncDF1, sncDF2, pw, UDAFUtils.query_1_UDAF)
        } catch {
          case e : Exception => {
            pw.println("When wrong return type given to function produce -> " + e.getMessage)
            pw.println("*     *     *     *     *     *     *     *     *     *")
            pw.flush()
          }
        }
        snc.sql(UDAFUtils.dropUDAF)

    /**
      * Create the UDAF, test it and drop it.
      * Create the UDAF with the same jar name but functionality of UDAF  will be different.
      * then test the UDAF no exception should occur then drop the UDAF.
      * Test that jar  gets undeployed cleanly from cluster.
      */
        try {
          pw.println("Test UDAF : jar gets undeployed cleanly from cluster.")
          snc.sql(UDAFUtils.createUDAF)
          val sncDF1 = snc.sql(UDAFUtils.query_1_UDAF)
          val sncDF2 = snc.sql(UDAFUtils.query_1_Snappy)
          validateResultSet(snc, sncDF1, sncDF2, pw, UDAFUtils.query_1_UDAF)
          snc.sql(UDAFUtils.dropUDAF)
          snc.sql(UDAFUtils.createUDAFWithDifferentFunctionality)
          val sncDF3 = snc.sql(UDAFUtils.query_3_UDAF)
          val sncDF4 = snc.sql(UDAFUtils.query_3_Snappy)
          validateResultSet(snc, sncDF3, sncDF4, pw, UDAFUtils.query_3_UDAF)
        } catch {
          case e : Exception => {
            pw.println("Undepoly jar cleanly from cluster produce -> " + e.getMessage)
            pw.println("*     *     *     *     *     *     *     *     *     *")
            pw.flush()
          }
        }
        snc.sql(UDAFUtils.dropUDAF)

        try {
          pw.println("Testing : Multiple UDAF in select statement")
          snc.sql(UDAFUtils.dropUDAF)
          snc.sql(UDAFUtils.dropCleverUDAF)
          snc.sql(UDAFUtils.dropDullUDAF)
          snc.sql(UDAFUtils.createUDAF)
          snc.sql(UDAFUtils.createCleverUDAF)
          snc.sql(UDAFUtils.createDullUDAF)
          val sncDF1 = snc.sql(UDAFUtils.query_mulitple_UDAF)
          sncDF1.show()
          val validateDF1 = sncDF1.select(sncDF1("class"), sncDF1("meanmarks(maths, class)"))
          val validateDF2 = sncDF1.select(sncDF1("class"), sncDF1("cleverstudent(maths)"))
          val validateDF3 = sncDF1.select(sncDF1("class"), sncDF1("dullstudent(maths, 0, 0)"))
          val validateDF4 = snc.sql("select class,avg(maths) " +
            "from student group by class order by class")
          val validateDF5 = snc.sql("select class,count(maths) " +
            "from student where maths >= 95.0 group by class order by class")
          val validateDF6 = snc.sql("select class,count(maths) " +
            "from student where maths < 30.0 group by class order by class")
          val diff1 = validateDF1.except(validateDF4)
          val diff2 = validateDF2.except(validateDF5)
          val diff3 = validateDF3.except(validateDF6)
          val diff4 = validateDF4.except(validateDF1)
          val diff5 = validateDF5.except(validateDF2)
          val diff6 = validateDF6.except(validateDF3)

          if((diff1.count() == 0 && diff4.count() == 0) &&
            (diff2.count() == 0 && diff5.count() == 0) &&
            (diff3.count() == 0 && diff6.count() == 0)) {
            pw.println("Select statement with multiple UDAF executed successfully.")
          } else {
            pw.println("Select statement with multiple UDAF failed.")
          }
          pw.println("*     *     *     *     *     *     *     *     *     *")
          pw.flush()

        } catch {
          case e : Exception => {
            pw.println("Exception in multiple UDF -> " + e.getMessage)
            pw.println("*     *     *     *     *     *     *     *     *     *")
            pw.flush()
          }
        }
        snc.sql(UDAFUtils.dropUDAF)
        snc.sql(UDAFUtils.dropCleverUDAF)
        snc.sql(UDAFUtils.dropDullUDAF)

        snc.sql(UDAFUtils.dropStagingStudent)
        snc.sql(UDAFUtils.dropStudent)
        pw.println("Smart Connector UDAF validation ends successfully...")
        println("Smart Connector UDAF validation ends successfully...")
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
