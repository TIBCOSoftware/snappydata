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
package org.apache.spark.sql.store

import java.io.File
import java.net.URL
import java.sql.DriverManager

import scala.util.{Failure, Success, Try}

import com.pivotal.gemfirexd.TestUtil
import io.snappydata.core.RefData
import io.snappydata.{Lead, ServiceManager, SnappyFunSuite}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{TestUtils, DynamicJarInstallationDUnitTest}
import org.apache.spark.TestUtils.JavaSourceFromString
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.{DataType, DataTypes}

case class OrderData(ref: Int, description: String, amount:Long)

class SnappyUDFTest extends SnappyFunSuite with BeforeAndAfterAll {

  val query = s"select strnglen(description) from RR_TABLE"
  var serverHostPort: String = null
  val currDir: File = new File(System.getProperty("user.dir"))

  def getJavaSourceFromString(name: String, code: String): JavaSourceFromString = {
    new JavaSourceFromString(name, code)
  }

  def createUDFClass(name: String, code: String): File = {
    TestUtils.createCompiledClass(name, currDir, getJavaSourceFromString(name, code), Seq.empty[URL])
  }

  def createJarFile(files: Seq[File]): String = {
    val jarFile = new File(currDir, "testJar-%s.jar".format(System.currentTimeMillis()))
    TestUtils.createJar(files, jarFile)
    jarFile.getName
  }

  override def beforeAll: Unit = {
    val rdd = sc.parallelize((1 to 5).map(i => OrderData(i, s"some $i", i)))
    val refDf = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS RR_TABLE")
    snc.sql("DROP TABLE IF EXISTS COL_TABLE")

    snc.sql("CREATE TABLE RR_TABLE(OrderRef INT NOT NULL, description String, price BIGINT)")
    snc.sql("CREATE TABLE COL_TABLE(OrderRef INT NOT NULL, description String, price  LONG) using column options()")

    refDf.write.insertInto("RR_TABLE")
    refDf.write.insertInto("COL_TABLE")
    serverHostPort = TestUtil.startNetServer()
  }

  override def afterAll: Unit = {
    snc.sql("DROP TABLE IF EXISTS RR_TABLE")
    snc.sql("DROP TABLE IF EXISTS COL_TABLE")
    TestUtil.stopNetServer()
  }

  private def detailedTest(): Unit = {
    assert(snc.snappySession.sessionCatalog.listFunctions("app", "str*").
        find(f => (f._1.toString().contains("strnglen"))).size == 1)


    assert(snc.snappySession.sql("DESCRIBE FUNCTION APP.strnglen").collect().length == 3)
    assert(snc.snappySession.sql("DESCRIBE FUNCTION EXTENDED APP.strnglen").collect().length == 4)
    assert(snc.snappySession.sql("DESCRIBE FUNCTION strnglen").collect().length == 3)
    assert(snc.snappySession.sql("DESCRIBE FUNCTION EXTENDED strnglen").collect().length == 4)
    assert(snc.snappySession.sql("SHOW FUNCTIONS strnglen").collect().length == 1)
    assert(snc.snappySession.sql("SHOW FUNCTIONS APP.strnglen").collect().length == 1)

    snc.snappySession.sql("DROP FUNCTION IF EXISTS app.strnglen")
    //Drop again to check if exists functionality
    snc.snappySession.sql("DROP FUNCTION IF EXISTS app.strnglen")

    Try(snc.sql(query).count()) match {
      case Success(df) => throw new AssertionError(" Should not have succeded with dropped function")
      case Failure(error) => // Do nothing
    }
  }

  test("Test UDF with Multiple  interface") {
    val udfText: String = "public class MultUDF implements org.apache.spark.sql.api.java.UDF1<Integer,Integer>, org.apache.spark.sql.api.java.UDF2<Integer,Integer, Integer> {" +
        " @Override public Integer call(Integer s){ " +
        "               return s; " +
        "}" +
        " @Override public Integer call(Integer s1, Integer s2){ " +
        "               return s1 + s2; " +
        "}" +
        "}"
    val file = createUDFClass("MultUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.multudf AS MultUDF " +
        s"RETURNS Integer USING FILE " +
        s"'$jar'")
    snc.sql("select multudf(OrderRef) from col_table").collect().foreach(r => println(r))
    snc.sql("select multudf(OrderRef, OrderRef) from col_table").collect().foreach(r => println(r))
  }

  test("Test UDF with Integer  Return type") {
    val udfText: String = "public class IntegerUDF implements org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return s.length(); " +
        "}" +
        "}"
    val file = createUDFClass("IntegerUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.intudf AS IntegerUDF " +
        s"RETURNS Integer USING FILE " +
        s"'$jar'")
    snc.sql("select intudf(description) from col_table").collect().foreach(r => println(r))
  }

  test("Test UDF with Long  Return type") {
    val udfText: String = "public class LongUDF implements org.apache.spark.sql.api.java.UDF1<Long,Long> {" +
        " @Override public Long call(Long s){ " +
        "               return s; " +
        "}" +
        "}"
    val file = createUDFClass("LongUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.longudf AS LongUDF " +
        s"RETURNS Long USING FILE " +
        s"'$jar'")
    snc.sql("select longudf(PRICE) from col_table").collect().foreach(r => println(r))
  }

  ignore("Test all UDFs") {
    // create a table with 22 cols with int type
    // insert 4 rows with all 1 values
    // create 22 udfs which will sum the cols. assertion will be udf1 = 1 udf2 = 2 etc

  }

  ignore("Test UDAFs") {

    snc.snappySession.sql(s"CREATE FUNCTION APP.mydoubleavg AS io.snappydata.udf.MyDoubleAvg")
    val query = s"select mydoubleavg(ORDERREF) from COL_TABLE"
    val udfdf = snc.sql(query)
    assert(udfdf.collect().apply(0)(0) == 103)
  }

  test("Test UDF with String  Return type") {
    val udfText: String = "public class StringUDF implements org.apache.spark.sql.api.java.UDF1<String,String> {" +
        " @Override public String call(String s){ " +
        "               return s + s; " +
        "}" +
        "}"
    val file = createUDFClass("StringUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.strudf AS StringUDF " +
        s"RETURNS STRING USING FILE " +
        s"'$jar'")
    snc.sql("select strudf(description) from col_table").collect().foreach(r => println(r))
  }

}
