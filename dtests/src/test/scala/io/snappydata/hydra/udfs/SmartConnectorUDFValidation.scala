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
        if(result.equals(new Timestamp(System.currentTimeMillis()).toString())) {
          pw.println("Java UDF13 -> " + result)
        } else {
          pw.println("Mismatch in Java UDF13 -> " + result)
        }
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
  }
}
