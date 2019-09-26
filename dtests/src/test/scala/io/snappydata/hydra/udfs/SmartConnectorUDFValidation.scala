/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") you
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
package io.snappydata.hydra.udfs

import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.Timestamp
import java.sql.Date
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SnappyContext, SparkSession}

object SmartConnectorUDFValidation {
  def main(args: Array[String]): Unit = {
    //  scalastyle:off println
    println("Smart connector Java / Scala UDF validation Job Started...")
    val connectionURL: String = args(args.length - 1)
    println("ConnectionURL : " + connectionURL)
    val conf: SparkConf = new SparkConf()
      .setAppName("UDF Interface Java / Scala Validation Job")
      .set("snappydata.connection", connectionURL)
    val sc: SparkContext = SparkContext.getOrCreate(conf)
    val snc: SnappyContext = SnappyContext(sc)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val outputFile = "ValidateUDFInterfaces_" + System.currentTimeMillis()
    val pw: PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), true))
    pw.println("Smart connector Java / Scala UDF validation Job Started...")
    val printDFContent: Boolean = false
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val udfJarPath = args(0)

    executeJavaUDF1(snc, udfJarPath, pw)
    executeJavaUDF2(snc, udfJarPath, pw)
    executeJavaUDF3(snc, udfJarPath, pw)
    executeJavaUDF4(snc, udfJarPath, pw)
    executeJavaUDF5(snc, udfJarPath, pw)
    executeJavaUDF6(snc, udfJarPath, pw)
    executeJavaUDF7(snc, udfJarPath, pw)
    executeJavaUDF8(snc, udfJarPath, pw)
    executeJavaUDF9(snc, udfJarPath, pw)
    executeJavaUDF10(snc, udfJarPath, pw)
    executeJavaUDF11(snc, udfJarPath, pw)
    executeJavaUDF12(snc, udfJarPath, pw)
    executeJavaUDF13(snc, udfJarPath, pw)
    executeJavaUDF14(snc, udfJarPath, pw)
    executeJavaUDF15(snc, udfJarPath, pw)
    executeJavaUDF16(snc, udfJarPath, pw)
    executeJavaUDF17(snc, udfJarPath, pw)
    executeJavaUDF18(snc, udfJarPath, pw)
    executeJavaUDF19(snc, udfJarPath, pw)
    executeJavaUDF20(snc, udfJarPath, pw)
    executeJavaUDF21(snc, udfJarPath, pw)
    executeJavaUDF22(snc, udfJarPath, pw)

    executeScalaUDF1(snc, udfJarPath, pw)
    executeScalaUDF2(snc, udfJarPath, pw)
    executeScalaUDF3(snc, udfJarPath, pw)
    executeScalaUDF4(snc, udfJarPath, pw)
    executeScalaUDF5(snc, udfJarPath, pw)
    executeScalaUDF6(snc, udfJarPath, pw)
    executeScalaUDF7(snc, udfJarPath, pw)
    executeScalaUDF8(snc, udfJarPath, pw)
    executeScalaUDF9(snc, udfJarPath, pw)
    executeScalaUDF10(snc, udfJarPath, pw)
    executeScalaUDF11(snc, udfJarPath, pw)
    executeScalaUDF12(snc, udfJarPath, pw)
    executeScalaUDF13(snc, udfJarPath, pw)
    executeScalaUDF14(snc, udfJarPath, pw)
    executeScalaUDF15(snc, udfJarPath, pw)
    executeScalaUDF16(snc, udfJarPath, pw)
    executeScalaUDF17(snc, udfJarPath, pw)
    executeScalaUDF18(snc, udfJarPath, pw)
    executeScalaUDF19(snc, udfJarPath, pw)
    executeScalaUDF20(snc, udfJarPath, pw)
    executeScalaUDF21(snc, udfJarPath, pw)
    executeScalaUDF22(snc, udfJarPath, pw)
    executeScalaUDF13_BadCase(snc, udfJarPath, pw)
    pw.println("Smart connector Java / Scala UDF validation Job Completed Successfully...")
    pw.close()

    def executeJavaUDF1(snc : SnappyContext, udfJarPath: String, pw : PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T1")
        snc.sql("CREATE TABLE IF NOT EXISTS T1(id Integer) USING COLUMN")
        snc.sql("INSERT INTO T1 VALUES(25)")
        snc.sql("CREATE FUNCTION UDF1 AS com.snappy.poc.udf.JavaUDF1 RETURNS INTEGER USING JAR '"
          + udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF1(id) FROM T1")
        var result = df1.collect().mkString
        result = result.replace("[", "").replace("]", "")
        if(result.equals("125")) {
          pw.println("Java UDF1 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF1 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF1")
        snc.sql("DROP TABLE IF EXISTS T1")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF1 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF2(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T2")
        snc.sql("CREATE TABLE IF NOT EXISTS T2(s1 String, s2 String) USING COLUMN")
        snc.sql("INSERT INTO T2 VALUES('Snappy','Data')")
        snc.sql("CREATE FUNCTION UDF2 AS com.snappy.poc.udf.JavaUDF2 RETURNS STRING USING JAR '"
          + udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF2(s1,s2) FROM T2")
        var result = df1.collect().mkString
        result = result.replace("[", "").replace("]", "")
        if(result.equals("Snappy#Data")) {
          pw.println("Java UDF2 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF2 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF2;");
        snc.sql("DROP TABLE IF EXISTS T2;");
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF2 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF3(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T3")
        snc.sql("CREATE TABLE IF NOT EXISTS T3" +
          "(arr1 Array<String>,arr2 Array<String>,arr3 Array<String>) USING COLUMN")
        snc.sql("INSERT INTO T3 SELECT Array('snappydata','udf1testing','successful')," +
          "Array('snappydata','udf2testing','successful')," +
          "Array('snappydata','udf3testing','successful')")
        snc.sql("CREATE FUNCTION UDF3 AS " +
          "com.snappy.poc.udf.JavaUDF3 RETURNS STRING USING JAR '" +  udfJarPath+ "';");
        val df1 = snc.sql("SELECT UDF3(arr1,arr2,arr3) FROM T3")
        var result = df1.collect().mkString
        result = result.replace("[", "").replace("]", "")
        if(result.equals("SNAPPYDATA UDF1TESTING SUCCESSFUL " +
          "SNAPPYDATA UDF2TESTING SUCCESSFUL " +
          "SNAPPYDATA UDF3TESTING SUCCESSFUL ")) {
          pw.println("Java UDF3 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF3 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF3")
        snc.sql("DROP TABLE IF EXISTS T3")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF3 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF4(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T4")
        snc.sql("CREATE TABLE IF NOT EXISTS T4" +
          "(m1 Map<String,Double>,m2 Map<String,Double>," +
          "m3 Map<String,Double>,m4 Map<String,Double>) USING COLUMN")
        snc.sql("INSERT INTO T4 SELECT " +
          "Map('Maths',100.0d),Map('Science',96.5d),Map('English',92.0d),Map('Music',98.5d)")
        snc.sql("CREATE FUNCTION UDF4 AS " +
          "com.snappy.poc.udf.JavaUDF4 RETURNS DOUBLE USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF4(m1, m2, m3, m4) FROM T4")
        var result = df1.collect().mkString
        result = result.replace("[", "").replace("]", "")
        var output = result.toFloat
        if(output == 387.0) {
          pw.println("Java UDF4 -> " + output)
        } else {
          pw.println("Mismatch in Java UDF4 -> " + output)
        }
        snc.sql("DROP FUNCTION UDF4")
        snc.sql("DROP TABLE IF EXISTS T4")
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF4 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF5(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T5")
        snc.sql("CREATE TABLE IF NOT EXISTS T5" +
          "(b1 Boolean,b2 Boolean,b3 Boolean,b4 Boolean,b5 Boolean) USING COLUMN")
        snc.sql("INSERT INTO T5 VALUES(true,true,true,true,true)")
        snc.sql("CREATE FUNCTION UDF5 AS " +
          "com.snappy.poc.udf.JavaUDF5 RETURNS STRING USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF5(b1,b2,b3,b4,b5) FROM T5")
        var result = df1.collect().mkString
        result = result.replace("[", "").replace("]", "")
        if(result.equals("0x:1f")) {
          pw.println("Java UDF5 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF5 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF5")
        snc.sql("DROP TABLE IF EXISTS T5")
      } catch {
        case e: Exception => {
          pw.println("Exception in Java UDF5 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF6(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T6")
        snc.sql("CREATE TABLE IF NOT EXISTS T6" +
          "(d1 Decimal,d2 Decimal,d3 Decimal,d4 Decimal,d5 Decimal,d6 Decimal) USING COLUMN")
        snc.sql("INSERT INTO T6 VALUES(231.45,563.93,899.25,611.98,444.44,999.99)")
        snc.sql("CREATE FUNCTION UDF6 AS " +
          "com.snappy.poc.udf.JavaUDF6 RETURNS DECIMAL USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF6(d1,d2,d3,d4,d5,d6) FROM T6")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("3751.040000000000000000")) {
          pw.println("Java UDF6 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF6 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF6")
        snc.sql("DROP TABLE IF EXISTS T6")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF6 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF7(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T7")
        snc.sql("CREATE TABLE IF NOT EXISTS T7" +
          "(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer," +
          "st Struct<ist:Integer,dst:Double,sst:String,snull:Integer>) USING COLUMN")
        snc.sql("INSERT INTO T7 SELECT 10,20,30,40,50,60,Struct(17933,54.68d,'SnappyData','')")
        snc.sql("CREATE FUNCTION UDF7 AS " +
          "com.snappy.poc.udf.JavaUDF7 RETURNS STRING USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF7(i1,i2,i3,i4,i5,i6,st) FROM T7")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("210(17933,54.68,SnappyData, IsNull :true)")) {
          pw.println("Java UDF7 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF7 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF7")
        snc.sql("DROP TABLE IF EXISTS T7")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF7 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF8(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T8")
        snc.sql("CREATE TABLE IF NOT EXISTS T8" +
          "(l1 Long,l2 Long,l3 Long,l4 Long,l5 Long,l6 Long,l7 Long,l8 Long) USING COLUMN")
        snc.sql("INSERT INTO T8 VALUES(10,20,30,40,50,60,70,80)")
        snc.sql("CREATE FUNCTION UDF8 AS " +
          "com.snappy.poc.udf.JavaUDF8 RETURNS SHORT USING JAR '" +  udfJarPath + "';");
        val df1 = snc.sql("SELECT UDF8(l1,l2,l3,l4,l5,l6,l7,l8) FROM T8")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("8")) {
          pw.println("Java UDF8 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF8 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF8")
        snc.sql("DROP TABLE IF EXISTS T8")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF8 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF9(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T9")
        snc.sql("CREATE TABLE IF NOT EXISTS T9" +
          "(c1 Varchar(9),c2 Varchar(9),c3 Varchar(9),c4 Varchar(9),c5 Varchar(9),c6 Varchar(9)," +
          "c7 Varchar(9),c8 Varchar(9),c9 Varchar(9)) USING COLUMN")
        snc.sql("INSERT INTO T9 VALUES('c','h','a','n','d','r','e','s','h')")
        snc.sql("CREATE FUNCTION UDF9 AS " +
          "com.snappy.poc.udf.JavaUDF9 RETURNS STRING USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF9(c1,c2,c3,c4,c5,c6,c7,c8,c9) FROM T9")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("chandresh")) {
          pw.println("Java UDF9 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF9 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF9")
        snc.sql("DROP TABLE IF EXISTS T9")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF9 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF10(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T10")
        snc.sql("CREATE TABLE IF NOT EXISTS T10" +
          "(f1 Float,f2 Float,f3 Float,f4 Float,f5 Float,f6 Float,f7 Float,f8 Float," +
          "f9 Float,f10 Float) USING COLUMN")
        snc.sql("INSERT INTO T10 VALUES(12.7,12.7,12.7,12.7,12.7,12.7,12.7,12.7,12.7,12.7)")
        snc.sql("CREATE FUNCTION UDF10 AS " +
          "com.snappy.poc.udf.JavaUDF10 RETURNS FLOAT USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF10(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10) FROM T10")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("126.999985")) {
          pw.println("Java UDF10 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF10 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF10")
        snc.sql("DROP TABLE IF EXISTS T10")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF10 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF11(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T11")
        snc.sql("CREATE TABLE IF NOT EXISTS T11(f1 Float,f2 Float,f3 Float,f4 Float,f5 Float," +
          "f6 Float,f7 Float,f8 Float,f9 Float,f10 Float,f11 Float) USING COLUMN")
        snc.sql("INSERT INTO T11 VALUES(12.1,12.2,12.3,12.4,12.5,12.6,12.7,12.8,12.9,14.1,12.8)")
        snc.sql("CREATE FUNCTION UDF11 AS " +
          "com.snappy.poc.udf.JavaUDF11 RETURNS BOOLEAN USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF11(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11) FROM T11")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("true")) {
          pw.println("Java UDF11 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF11 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF11")
        snc.sql("DROP TABLE IF EXISTS T11")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF11 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF12(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T12")
        snc.sql("CREATE TABLE IF NOT EXISTS T12" +
          "(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String," +
          "s8 String,s9 String,s10 String,s11 String,s12 String) USING COLUMN")
        snc.sql("INSERT INTO T12 VALUES('Snappy','Data','Leader','Locator','Server'," +
          "'Cluster','Performance','UI','SQL','DataFrame','Dataset','API')")
        snc.sql("CREATE FUNCTION UDF12 AS " +
          "com.snappy.poc.udf.JavaUDF12 RETURNS DATE USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF12(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12) FROM T12")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        pw.println(result)
        if(result.equals(new Date(System.currentTimeMillis()).toString())) {
          pw.println("Java UDF12 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF12 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF12")
        snc.sql("DROP TABLE IF EXISTS T12")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF12 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF13(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T13")
        snc.sql("CREATE TABLE IF NOT EXISTS T13" +
          "(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer,i7 Integer," +
          "i8 Integer,i9 Integer,i10 Integer,i11 Integer,i12 Integer,i13 Integer) USING COLUMN")
        snc.sql("INSERT INTO T13 VALUES(10,21,25,54,89,78,63,78,55,13,14,85,71)")
        snc.sql("CREATE FUNCTION UDF13 AS " +
          "com.snappy.poc.udf.JavaUDF13 RETURNS TIMESTAMP USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF13(i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13) FROM T13")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        pw.println("Java UDF13 -> " + result)
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF13")
        snc.sql("DROP TABLE IF EXISTS T13")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF13 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF14(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T14")
        snc.sql("CREATE TABLE IF NOT EXISTS T14" +
          "(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String," +
          "s9 String,s10 String,s11 String,s12 String,s13 String,s14 String) USING COLUMN")
        snc.sql("INSERT INTO T14 VALUES('0XFF','10','067','54534','0X897'," +
          "'999','077','1234567891234567','0x8978','015','1005','13177','14736','07777')")
        snc.sql("CREATE FUNCTION UDF14 AS " +
          "com.snappy.poc.udf.JavaUDF14 RETURNS LONG USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF14(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14) FROM T14")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("10")) {
          pw.println("Java UDF14 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF14 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF14")
        snc.sql("DROP TABLE IF EXISTS T14")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF14 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF15(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T15")
        snc.sql("CREATE TABLE IF NOT EXISTS T15" +
          "(d1 Double,d2 Double,d3 Double,d4 Double,d5 Double,d6 Double,d7 Double," +
          "d8 Double,d9 Double,d10 Double,d11 Double,d12 Double," +
          "d13 Double,d14 Double,d15 Double) USING COLUMN")
        snc.sql("INSERT INTO T15 VALUES" +
          "(5.23,99.3,115.6,0.9,78.5,999.99,458.32,133.58,789.87," +
          "269.21,333.96,653.21,550.32,489.14,129.87)")
        snc.sql("CREATE FUNCTION UDF15 AS " +
          "com.snappy.poc.udf.JavaUDF15 RETURNS DOUBLE USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF15(d1,d2,d3,d4,d5,d6,d7,d8," +
          "d9,d10,d11,d12,d13,d14,d15) FROM T15")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("999.99")) {
          pw.println("Java UDF15 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF15 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF15")
        snc.sql("DROP TABLE IF EXISTS T15")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF15 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF16(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T16")
        snc.sql("CREATE TABLE IF NOT EXISTS T16" +
          "(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String," +
          "s9 String,s10 String,s11 String,s12 String,s13 String,s14 String," +
          "s15 String,s16 String) USING COLUMN")
        snc.sql("INSERT INTO T16 VALUES('snappy','data','is','working','on','a'," +
          "'distributed','data','base.','Its','Performance','is','faster','than','Apache','spark')")
        snc.sql("CREATE FUNCTION UDF16 AS " +
          "com.snappy.poc.udf.JavaUDF16 RETURNS VARCHAR(255) USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF16(s1,s2,s3,s4,s5,s6,s7,s8," +
          "s9,s10,s11,s12,s13,s14,s15,s16) FROM T16")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("snappy+data+is+working+on+a+distributed+data+base." +
          "+Its+Performance+is+faster+than+Apache+spark")) {
          pw.println("Java UDF16 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF16 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF16;");
        snc.sql("DROP TABLE IF EXISTS T16;");
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF16 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF17(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T17")
        snc.sql("CREATE TABLE IF NOT EXISTS T17" +
          "(f1 Float,f2 Float,f3 Float,f4 Float,f5 Float,f6 Float,f7 Float" +
          ",f8 Float,f9 Float,f10 Float,f11 Float,f12 Float,f13 Float,f14 Float," +
          "f15 Float,f16 Float,f17 Float) USING COLUMN")
        snc.sql("INSERT INTO T17 VALUES" +
          "(15.2,17.2,99.9,45.2,71.0,89.8,66.6,23.7," +
          "33.1,13.5,77.7,38.3,44.4,21.7,41.8,83.2,78.1)")
        snc.sql("CREATE FUNCTION UDF17 AS " +
          "com.snappy.poc.udf.JavaUDF17 RETURNS BOOLEAN USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF17(f1,f2,f3,f4,f5,f6,f7," +
          "f8,f9,f10,f11,f12,f13,f14,f15,f16,f17) FROM T17")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("true")) {
          pw.println("Java UDF17 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF17 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF17")
        snc.sql("DROP TABLE IF EXISTS T17")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF17 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF18(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T18")
        snc.sql("CREATE TABLE IF NOT EXISTS T18" +
          "(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String," +
          "s9 String,s10 String,s11 String,s12 String,s13 String,s14 String,s15 String," +
          "s16 String,s17 String,s18 String) USING COLUMN")
        snc.sql("INSERT INTO T18 VALUES" +
          "('snappy','data','spark','leader','locator','server','memory','eviction'," +
          "'sql','udf','udaf','scala','docker','catalog','smart connector'," +
          "'cores','executor','dir')")
        snc.sql("CREATE FUNCTION UDF18 AS " +
          "com.snappy.poc.udf.JavaUDF18 RETURNS STRING USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF18(s1,s2,s3,s4,s5,s6,s7,s8," +
          "s9,s10,s11,s12,s13,s14,s15,s16,s17,s18) FROM T18")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("executorsmart connectorcatalogdocker" +
          "evictionmemoryserverlocatorleadersnappy,,,,,,,,,,")) {
          pw.println("Java UDF18 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF18 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF18")
        snc.sql("DROP TABLE IF EXISTS T18")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF18 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF19(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T19")
        snc.sql("CREATE TABLE IF NOT EXISTS T19" +
          "(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String," +
          "s9 String,s10 String,s11 String,s12 String,s13 String,s14 String,s15 String," +
          "s16 String,s17 String,s18 String,s19 String) USING COLUMN")
        snc.sql("INSERT INTO T19 VALUES" +
          "('snappy','data','spark','leader','locator','server','memory','eviction'," +
          "'sql','udf','udaf','scala','docker','catalog','smart connector'," +
          "'cores','executor','dir','avro')")
        snc.sql("CREATE FUNCTION UDF19 AS " +
          "com.snappy.poc.udf.JavaUDF19 RETURNS STRING USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF19(s1,s2,s3,s4,s5,s6,s7,s8," +
          "s9,s10,s11,s12,s13,s14,s15,s16,s17,s18,s19) FROM T19")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("SNAPPY,DATA,SPARK,LEADER,LOCATOR,SERVER,MEMORY," +
          "EVICTION,SQL,UDF,UDAF,SCALA,DOCKER,CATALOG," +
          "SMART CONNECTOR,CORES,EXECUTOR,DIR,AVRO,")) {
          pw.println("Java UDF19 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF19 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF19")
        snc.sql("DROP TABLE IF EXISTS T19")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF19 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF20(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T20")
        snc.sql("CREATE TABLE IF NOT EXISTS T20" +
          "(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer," +
          "i7 Integer,i8 Integer,i9 Integer,i10 Integer,i11 Integer,i12 Integer," +
          "i13 Integer,i14 Integer,i15 Integer,i16 Integer,i17 Integer,i18 Integer," +
          "i19 Integer,i20 Integer) USING COLUMN")
        snc.sql("INSERT INTO T20 VALUES" +
          "(10,20,30,40,50,60,70,80,90," +
          "100,13,17,21,25,19,22,86,88,64,58)")
        snc.sql("CREATE FUNCTION UDF20 AS " +
          "com.snappy.poc.udf.JavaUDF20 RETURNS INTEGER USING JAR '" + udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF20(i1,i2,i3,i4,i5,i6,i7,i8,i9,i10," +
          "i11,i12,i13,i14,i15,i16,i17,i18,i19,i20) FROM T20")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("15")) {
          pw.println("Java UDF20 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF20 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF20")
        snc.sql("DROP TABLE IF EXISTS T20")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF20 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF21(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T21")
        snc.sql("CREATE TABLE IF NOT EXISTS T21" +
          "(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String," +
          "s8 String,s9 String,s10 String,s11 String,s12 String,s13 String,s14 String," +
          "s15 String,s16 String,s17 String,s18 String,s19 String," +
          "s20 String,s21 String) USING COLUMN")
        snc.sql("INSERT INTO T21 VALUES" +
          "('snappy','snappy','snappy','snappy','snappy','snappy'," +
          "'snappy','snappy','snappy','snappy','snappy','snappy'," +
          "'snappy','snappy','snappy','snappy','snappy','snappy'," +
          "'snappy','snappy','snappy')")
        snc.sql("CREATE FUNCTION UDF21 AS " +
          "com.snappy.poc.udf.JavaUDF21 RETURNS INTEGER USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF21(s1,s2,s3,s4,s5,s6,s7,s8," +
          "s9,s10,s11,s12,s13,s14,s15,s16,s17,s18,s19,s20,s21) FROM T21")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("126")) {
          pw.println("Java UDF21 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF21 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF21")
        snc.sql("DROP TABLE IF EXISTS T21")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF21 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeJavaUDF22(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS T22")
        snc.sql("CREATE TABLE IF NOT EXISTS T22" +
          "(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer,i7 Integer," +
          "i8 Integer,i9 Integer,i10 Integer,i11 Integer,i12 Integer,i13 Integer,i14 Integer," +
          "i15 Integer,i16 Integer,i17 Integer,i18 Integer,i19 Integer," +
          "i20 Integer,i21 Integer,i22 Integer) USING COLUMN")
        snc.sql("INSERT INTO T22 VALUES" +
          "(10,10,10,10,10,10,10,10,10," +
          "10,10,10,10,10,10,10,10,10,10,10,10,10)")
        snc.sql("CREATE FUNCTION UDF22 AS " +
          "com.snappy.poc.udf.JavaUDF22 RETURNS INTEGER USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT UDF22(i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11," +
          "i12,i13,i14,i15,i16,i17,i18,i19,i20,i21,i22) FROM T22")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("220")) {
          pw.println("Java UDF22 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF22 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION UDF22")
        snc.sql("DROP TABLE IF EXISTS T22")
      } catch {
        case e : Exception => {
          pw.println("Exception in Java UDF22 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF1(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST1")
        snc.sql("CREATE TABLE IF NOT EXISTS ST1(s1 String) USING COLUMN")
        snc.sql("INSERT INTO ST1 VALUES('snappyData')")
        snc.sql("CREATE FUNCTION ScalaUDF1 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF1 RETURNS TIMESTAMP USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF1(s1) FROM ST1")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        pw.println("Scala UDF1 -> " + result)
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF1")
        snc.sql("DROP TABLE IF EXISTS ST1")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF1 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF2(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST2")
        snc.sql("CREATE TABLE IF NOT EXISTS ST2(ts1 TimeStamp,ts2 TimeStamp) USING COLUMN")
        snc.sql("INSERT INTO ST2 VALUES" +
          "(CAST('2019-02-17 08:10:15' AS TIMESTAMP),CAST('2019-02-18 10:10:15' AS TIMESTAMP))")
        snc.sql("CREATE FUNCTION ScalaUDF2 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF2 RETURNS STRING USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF2(ts1,ts2) FROM ST2")
        val result = df1.collect().mkString.replace("[", "").replace("]", "")
        val output = "Difference is : \nHours : 26\nMinutes : 0\nSeconds : 0"
        if(result.equals(output)) {
          pw.println("Scala UDF2 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF2 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF2")
        snc.sql("DROP TABLE IF EXISTS ST2")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF2 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF3(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST3")
        snc.sql("CREATE TABLE IF NOT EXISTS ST3(d1 Double,d2 Double,d3 Double) USING COLUMN")
        snc.sql("INSERT INTO ST3 VALUES(15.3,15.3,15.3)")
        snc.sql("CREATE FUNCTION ScalaUDF3 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF3 RETURNS STRING USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF3(d1,d2,d3) FROM ST3")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        val output = "Sine :0.39674057313061206\n" +
          "Cosine : -0.9179307804142932\n" +
          " Tangent : -0.4322118634604993"
        if(result.equals(output)) {
          pw.println("Scala UDF3 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF3 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF3")
        snc.sql("DROP TABLE IF EXISTS ST3")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF3 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF4(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST4")
        snc.sql("CREATE TABLE IF NOT EXISTS ST4" +
          "(i1 Integer,i2 Integer,i3 Integer,i4 Integer) USING COLUMN")
        snc.sql("INSERT INTO ST4 VALUES(1024,2048,4096,8192)")
        snc.sql("CREATE FUNCTION ScalaUDF4 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF4 RETURNS DOUBLE USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF4(i1,i2,i3,i4) FROM ST4")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("231.76450198781714")) {
          pw.println("Scala UDF4 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF4 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF4")
        snc.sql("DROP TABLE IF EXISTS ST4")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF4 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF5(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST5")
        snc.sql("CREATE TABLE IF NOT EXISTS ST5" +
          "(i1 Integer,i2 Integer,i3 Integer,i4 Integer," +
          "st Struct<ist:Integer,dst:Double,sst:String,snull:Integer>) USING COLUMN")
        snc.sql("INSERT INTO ST5 SELECT 10,20,30,40,Struct(17933,54.68,'SnappyData','')")
        snc.sql("CREATE FUNCTION ScalaUDF5 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF5 RETURNS STRING USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF5(i1,i2,i3,i4,st) FROM ST5")
        val result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("Sum of four integer : 100, " +
          "Struct data type element : (17933,54.68,SnappyData,true)")) {
          pw.println("Scala UDF5 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF5 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF5")
        snc.sql("DROP TABLE IF EXISTS ST5")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF5 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF6(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST6")
        snc.sql("CREATE TABLE IF NOT EXISTS ST6" +
          "(m1 Map<String,Double>,m2 Map<String,Double>,m3 Map<String,Double>," +
          "m4 Map<String,Double>,m5 Map<String,Double>, m6 Map<String,Double>) " +
          "USING COLUMN")
        snc.sql("INSERT INTO ST6 SELECT " +
          "Map('Maths',98.7),Map('Science',96.1),Map('English',89.5)," +
          "Map('Social Studies',88.0),Map('Computer',95.4),Map('Music',92.5)")
        snc.sql("CREATE FUNCTION ScalaUDF6 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF6 RETURNS DOUBLE USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF6(m1,m2,m3,m4,m5,m6) FROM ST6")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("560.2")) {
          pw.println("Scala UDF6 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF6 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF6")
        snc.sql("DROP TABLE IF EXISTS ST6")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF6 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF7(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST7")
        snc.sql("CREATE TABLE IF NOT EXISTS ST7" +
          "(arr1 Array<Double>,arr2 Array<Double>,arr3 Array<Double>,arr4 Array<Double>," +
          "arr5 Array<Double>,arr6 Array<Double>,arr7 Array<Double>) USING COLUMN")
        snc.sql("INSERT INTO ST7 SELECT Array(29.8,30.7,12.3,25.9)," +
          "Array(17.3,45.3,32.6,24.1,10.7,23.8,78.8),Array(0.0,12.3,33.6,65.9,78.9,21.1)," +
          "Array(12.4,99.9),Array(23.4,65.6,52.1,32.6,85.6),Array(25.6,52.3,87.4),Array(30.2)")
        snc.sql("CREATE FUNCTION ScalaUDF7 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF7 RETURNS STRING USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF7(arr1,arr2,arr3,arr4,arr5,arr6,arr7) FROM ST7")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        val output = "V1 : 24.674999999999997\n V2 : 33.22857142857142\n " +
          "V2 : 35.300000000000004\n V2 : 56.150000000000006\n " +
          "V2 : 51.85999999999999\n V2 : 55.1\n V2 : 30.2"
        if(result.equals(output)) {
          pw.println("Scala UDF7 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF7 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF7")
        snc.sql("DROP TABLE IF EXISTS ST7")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF7 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF8(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST8")
        snc.sql("CREATE TABLE IF NOT EXISTS ST8" +
          "(s1 String,s2 String,s3 String,s4 String,s5 String," +
          "s6 String,s7 String,s8 String) USING COLUMN")
        snc.sql("INSERT INTO ST8 VALUES" +
          "('Spark','Snappy','GemXD','Docker','AWS','Scala','JIRA','Git')")
        snc.sql("CREATE FUNCTION ScalaUDF8 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF8 RETURNS STRING USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF8(s1,s2,s3,s4,s5,s6,s7,s8) FROM ST8")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        val output = "HashMap Size -> 8\n" +
          "Spark->Computation Engine\n" +
          "Snappy->In-memory database\n" +
          "GemXD->Storage layer\n" +
          "Docker->Container Platform\n" +
          "AWS->Cloud Platform\n" +
          "Scala->Programming Language\n" +
          "JIRA->Bug/Task tracking tool\n" +
          "Git->Version control tool"
        if(result.equals(output)) {
          pw.println("Scala UDF8 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF8 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF8")
        snc.sql("DROP TABLE IF EXISTS ST8")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF8 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF9(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST9")
        snc.sql("CREATE TABLE IF NOT EXISTS ST9" +
          "(b1 Boolean,b2 Boolean,b3 Boolean,b4 Boolean," +
          "b5 Boolean,b6 Boolean,b7 Boolean,b8 Boolean," +
          "b9 Boolean) USING COLUMN")
        snc.sql("INSERT INTO ST9 VALUES(false,false,true,false,false,true,true,true,false)")
        snc.sql("CREATE FUNCTION ScalaUDF9 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF9 RETURNS STRING USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF9(b1,b2,b3,b4,b5,b6,b7,b8,b9) FROM ST9")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        val output = "false && false -> false\n" +
          "false && true -> false\n" +
          "true && false -> false\n" +
          "true && true -> true\n" +
          "false || false -> false\n" +
          "false || true -> true\n" +
          "true || false -> true\n" +
          "true || true -> true\n" +
          "!(true) Or  -> true"
        if(result.equals(output)) {
          pw.println("Scala UDF9 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF9 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF9")
        snc.sql("DROP TABLE IF EXISTS ST9")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF9 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF10(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST10")
        snc.sql("CREATE TABLE IF NOT EXISTS ST10" +
          "(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer," +
          "i7 Integer,i8 Integer,i9 Integer,i10 Integer) USING COLUMN")
        snc.sql("INSERT INTO ST10 VALUES(2,4,6,8,10,12,14,16,18,20)")
        snc.sql("CREATE FUNCTION ScalaUDF10 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF10 RETURNS STRING USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF10(i1,i2,i3,i4,i5,i6,i7,i8,i9,i10) FROM ST10")
        val result = df1.collect().mkString.replace("[", "").replace("]", "")
        val output = "(2,24,720,40320,3628800,479001600,87178291200," +
          "20922789888000,6402373705728000,2432902008176640000)"
        if(result.equals(output)) {
          pw.println("Scala UDF10 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF10 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF10")
        snc.sql("DROP TABLE IF EXISTS ST10")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF10 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF11(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST11")
        snc.sql("CREATE TABLE IF NOT EXISTS ST11" +
          "(f1 Float,f2 Float,f3 Float,f4 Float,f5 Float,f6 Float," +
          "f7 Float,f8 Float,f9 Float,f10 Float,f11 Float) USING COLUMN")
        snc.sql("INSERT INTO ST11 VALUES(5.1,5.2,5.3,5.4,5.5,5.6,5.7,5.8,5.9,6.3,8.7)")
        snc.sql("CREATE FUNCTION ScalaUDF11 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF11 RETURNS FLOAT USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF11(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11) FROM ST11")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("2.49924368E8")) {
          pw.println("Scala UDF11 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF11 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF11")
        snc.sql("DROP TABLE IF EXISTS ST11")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF11 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF12(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST12")
        snc.sql("CREATE TABLE IF NOT EXISTS ST12" +
          "(s1 String,i1 Integer,i2 Integer,s2 String,l1 Long,l2 Long," +
          "s3 String,f1 Float,f2 Float,s4 String, sh1 Short,sh2 Short) USING COLUMN")
        snc.sql("INSERT INTO ST12 VALUES" +
          "('+',15,30,'-',30,15,'*',10.5,10.5,'/',smallint(12),smallint(12))")
        snc.sql("CREATE FUNCTION ScalaUDF12 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF12 RETURNS STRING USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF12(s1,i1,i2,s2,l1,l2,s3,f1,f2,s4,sh1,sh2) FROM ST12")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("Addition -> 45 , Substraction -> 15 , " +
          "Multiplication -> 110.25 , Division -> 1")) {
          pw.println("Scala UDF12 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF12 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF12")
        snc.sql("DROP TABLE IF EXISTS ST12")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF12 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF13(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST13")
        snc.sql("CREATE TABLE IF NOT EXISTS ST13" +
          "(d1 Decimal(5,2),d2 Decimal(5,2),d3 Decimal(5,2),d4 Decimal(5,2),d5 Decimal(5,2)," +
          "d6 Decimal(5,2),d7 Decimal(5,2),d8 Decimal(5,2),d9 Decimal(5,2),d10 Decimal(5,2)," +
          "d11 Decimal(5,2),d12 Decimal(5,2),d13 Decimal(5,2)) USING COLUMN")
        snc.sql("INSERT INTO ST13 VALUES" +
          "(123.45,123.45,123.45,123.45,123.45,123.45,123.45," +
          "123.45,123.45,123.45,123.45,123.45,123.45)")
        snc.sql("CREATE FUNCTION ScalaUDF13 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF13 RETURNS DECIMAL USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF13(d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13) FROM ST13")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("160485.000000000000000000")) {
          pw.println("Scala UDF13 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF13 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF13")
        snc.sql("DROP TABLE IF EXISTS ST13")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF13 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF13_BadCase(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST13")
        snc.sql("CREATE TABLE IF NOT EXISTS ST13" +
          "(d1 Decimal(5,2),d2 Decimal(5,2),d3 Decimal(5,2),d4 Decimal(5,2)," +
          "d5 Decimal(5,2),d6 Decimal(5,2),d7 Decimal(5,2),d8 Decimal(5,2)," +
          "d9 Decimal(5,2),d10 Decimal(5,2),d11 Decimal(5,2),d12 Decimal(5,2)," +
          "d13 Decimal(5,2)) USING COLUMN")
        snc.sql("INSERT INTO ST13 VALUES" +
          "(123.45,123.45,123.45,123.45,123.45,123.45," +
          "123.45,123.45,123.45,123.45,123.45,123.45,123.45)")
        snc.sql("CREATE FUNCTION ScalaUDF13_1 AS " +
          "com.snappy.scala.poc.udf.BadCase_ScalaUDF13 " +
          "RETURNS DECIMAL USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF13_1" +
          "(d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13) FROM ST13")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF13 Bad Case : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      } finally {
        snc.sql("DROP TABLE IF EXISTS ST13")
        snc.sql("DROP FUNCTION IF EXISTS ScalaUDF13_1")
      }
    }

    def executeScalaUDF14(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST14")
        snc.sql("CREATE TABLE IF NOT EXISTS ST14" +
          "(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String," +
          "s7 String,s8 String,s9 String,s10 String,s11 String,s12 String," +
          "s13 String,s14 String) USING COLUMN")
        snc.sql("INSERT INTO ST14 VALUES('s','n','a','p','p','y','d','a','t','a','a','s','D','B')")
        snc.sql("CREATE FUNCTION ScalaUDF14 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF14 RETURNS STRING USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF14" +
          "(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14) FROM ST14")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("SnappyData As Spark DB")) {
          pw.println("Scala UDF14 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF14 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF14")
        snc.sql("DROP TABLE IF EXISTS ST14")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF14 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF15(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST15")
        snc.sql("CREATE TABLE IF NOT EXISTS ST15" +
          "(b1 Boolean,b2 Boolean,b3 Boolean,b4 Boolean,b5 Boolean,b6 Boolean," +
          "b7 Boolean,b8 Boolean,b9 Boolean,b10 Boolean,b11 Boolean,b12 Boolean," +
          "b13 Boolean,b14 Boolean,b15 Boolean) USING COLUMN")
        snc.sql("INSERT INTO ST15 VALUES" +
          "(true,false,true,false,true,false,true,false,true,false,true,false,true,false,true)")
        snc.sql("CREATE FUNCTION ScalaUDF15 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF15 RETURNS INTEGER USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF15" +
          "(b1,b2,b3,b4,b5,b6,b7,b8,b9,b10,b11,b12,b13,b14,b15) FROM ST15")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("21845")) {
          pw.println("Scala UDF15 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF15 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF15")
        snc.sql("DROP TABLE IF EXISTS ST15")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF15 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF16(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST16")
        snc.sql("CREATE TABLE IF NOT EXISTS ST16" +
          "(b1 Byte,b2 Byte,b3 Byte,b4 Byte,b5 Byte,b6 Byte," +
          "b7 Byte,b8 Byte,b9 Byte,b10 Byte,b11 Byte,b12 Byte," +
          "b13 Byte,b14 Byte,b15 Byte,b16 Byte) USING COLUMN")
        snc.sql("INSERT INTO ST16 VALUES" +
          "(TINYINT(1),TINYINT(2),TINYINT(3),TINYINT(4),TINYINT(5),TINYINT(6)," +
          "TINYINT(7),TINYINT(8),TINYINT(9),TINYINT(10),TINYINT(11),TINYINT(12)," +
          "TINYINT(13),TINYINT(14),TINYINT(15),TINYINT(16))")
        snc.sql("CREATE FUNCTION ScalaUDF16 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF16 RETURNS BOOLEAN USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF16" +
          "(b1,b2,b3,b4,b5,b6,b7,b8,b9,b10,b11,b12,b13,b14,b15,b16) FROM ST16")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("true")) {
          pw.println("Scala UDF16 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF16 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF16")
        snc.sql("DROP TABLE IF EXISTS ST16")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF16 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF17(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST17")
        snc.sql("CREATE TABLE IF NOT EXISTS ST17" +
          "(l1 Long,l2 Long,l3 Long,l4 Long,l5 Long,l6 Long,l7 Long," +
          "l8 Long,l9 Long,l10 Long,l11 Long,l12 Long,l13 Long," +
          "l14 Long,l15 Long,l16 Long,l17 Long) USING COLUMN")
        snc.sql("INSERT INTO ST17 VALUES" +
          "(125401,20456789,1031425,2000000,787321654,528123085,14777777," +
          "33322211145,4007458725,27712345678,666123654,1005," +
          "201020092008,1500321465,888741852,963852147,444368417)")
        snc.sql("CREATE FUNCTION ScalaUDF17 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF17 RETURNS LONG USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF17" +
          "(l1,l2,l3,l4,l5,l6,l7,l8,l9,l10,l11,l12,l13,l14,l15,l16,l17) FROM ST17")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("271879352227")) {
          pw.println("Scala UDF17 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF17 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF17")
        snc.sql("DROP TABLE IF EXISTS ST17")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF17 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF18(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST18")
        snc.sql("CREATE TABLE IF NOT EXISTS ST18" +
          "(l1 Long,l2 Long,l3 Long,l4 Long,l5 Long,l6 Long," +
          "l7 Long,l8 Long,l9 Long,l10 Long,l11 Long,l12 Long," +
          "l13 Long,l14 Long,l15 Long,l16 Long,l17 Long,l18 Long) USING COLUMN")
        snc.sql("INSERT INTO ST18 VALUES" +
          "(125,204,103,20,787,528,147,333,400,277,666" +
          ",1005,2010,1500,888,963,444,777)")
        snc.sql("CREATE FUNCTION ScalaUDF18 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF18 RETURNS DATE USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF18" +
          "(l1,l2,l3,l4,l5,l6,l7,l8,l9,l10,l11,l12,l13,l14,l15,l16,l17,l18) FROM ST18")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        val time = new Date(System.currentTimeMillis()).toString()
        if(result.equals(time)) {
          pw.println("Scala UDF18 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF18 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF18")
        snc.sql("DROP TABLE IF EXISTS ST18")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF18 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF19(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST19")
        snc.sql("CREATE TABLE IF NOT EXISTS ST19" +
          "(d1 Double,d2 Double,d3 Double,d4 Double,d5 Double,d6 Double," +
          "d7 Double,d8 Double,d9 Double,d10 Double,d11 Double,d12 Double," +
          "d13 Double,d14 Double,d15 Double,d16 Double,d17 Double,d18 Double," +
          "d19 Double) USING COLUMN")
        snc.sql("INSERT INTO ST19 VALUES" +
          "(25.3,200.8,101.5,201.9,789.85,522.398,144.2,336.1," +
          "400.0,277.7,666.6,1005.630,2010.96,1500.21,888.7," +
          "963.87,416.0,786.687,1.1)")
        snc.sql("CREATE FUNCTION ScalaUDF19 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF19 RETURNS DOUBLE USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF19" +
          "(d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19) FROM ST19")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("11239.505000000001")) {
          pw.println("Scala UDF19 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF19 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF19")
        snc.sql("DROP TABLE IF EXISTS ST19")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF19 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF20(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST20")
        snc.sql("CREATE TABLE IF NOT EXISTS ST20" +
          "(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String," +
          "s7 String,s8 String,s9 String,s10 String,s11 String,s12 String," +
          "s13 String,s14 String,s15 String,s16 String,s17 String,s18 String," +
          "s19 String,s20 String) USING COLUMN")
        snc.sql("INSERT INTO ST20 VALUES" +
          "(' Snappy ','   Data   ','  is  ',' the ','  first    ','   product'," +
          "'to ',' integrate   '," +
          "'a',' database','  into  ','  spark  ','making      ','   Spark   '," +
          "'  work  ','just    ','like','    a','         database   ',' . ')")
        snc.sql("CREATE FUNCTION ScalaUDF20 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF20 RETURNS STRING USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF20" +
          "(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12," +
          "s13,s14,s15,s16,s17,s18,s19,s20) FROM ST20")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("Snappy,Data,is,the,first,product,to,integrate,a," +
          "database,into,spark,making,Spark,work,just,like,a,database.")) {
          pw.println("Scala UDF20 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF20 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF20")
        snc.sql("DROP TABLE IF EXISTS ST20")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF20 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF21(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST21")
        snc.sql("CREATE TABLE IF NOT EXISTS ST21" +
          "(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String," +
          "s7 String,s8 String,s9 String,s10 String,s11 String,s12 String," +
          "s13 String,s14 String,s15 String,s16 String,s17 String,s18 String" +
          ",s19 String,s20 String,s21 String) USING COLUMN")
        snc.sql("INSERT INTO ST21 VALUES" +
          "('23','21','30','37','25','22','28','32'," +
          "'31','24','24','26','35','89','98','45'," +
          "'54','95','74','66','5')")
        snc.sql("CREATE FUNCTION ScalaUDF21 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF21 RETURNS FLOAT USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF21" +
          "(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12," +
          "s13,s14,s15,s16,s17,s18,s19,s20,s21) FROM ST21")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("42.095238")) {
          pw.println("Scala UDF21 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF21 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF21")
        snc.sql("DROP TABLE IF EXISTS ST21")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF21 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

    def executeScalaUDF22(snc: SnappyContext, udfJarPath: String, pw: PrintWriter): Unit = {
      try {
        snc.sql("DROP TABLE IF EXISTS ST22")
        snc.sql("CREATE TABLE IF NOT EXISTS ST22" +
          "(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer," +
          "i7 Integer,i8 Integer,i9 Integer,i10 Integer,i11 Integer,i12 Integer," +
          "i13 Integer,i14 Integer,i15 Integer,i16 Integer,i17 Integer,i18 Integer," +
          "i19 Integer,i20 Integer,i21 Integer,i22 Integer) USING COLUMN")
        snc.sql("INSERT INTO ST22 VALUES" +
          "(2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2)")
        snc.sql("CREATE FUNCTION ScalaUDF22 AS " +
          "com.snappy.scala.poc.udf.ScalaUDF22 RETURNS INTEGER USING JAR '" +  udfJarPath + "';")
        val df1 = snc.sql("SELECT ScalaUDF22" +
          "(i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15," +
          "i16,i17,i18,i19,i20,i21,i22) FROM ST22")
        var result = df1.collect().mkString.replace("[", "").replace("]", "")
        if(result.equals("4194304")) {
          pw.println("Scala UDF22 -> " + result)
        } else {
          pw.println("Mismatch in Scala UDF22 -> " + result)
        }
        pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        pw.flush()
        snc.sql("DROP FUNCTION ScalaUDF22")
        snc.sql("DROP TABLE IF EXISTS ST22")
      } catch {
        case e : Exception => {
          pw.println("Exception in Scala UDF22 : " + e.getMessage)
          pw.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
          pw.flush()
        }
      }
    }

  }
}
