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
package org.apache.spark.sql.udf

import java.io.File
import java.net.URL

import scala.util.{Failure, Success, Try}

import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.TestUtils
import org.apache.spark.TestUtils.JavaSourceFromString
import UserDefinedFunctionsDUnitTest._
import org.apache.spark.sql.SnappySession

case class OrderData(ref: Int, description: String, amount: Long)

class UserDefinedFunctionsDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) {

  def createTables(session: SnappySession) {
    val snSession = session
    val rdd = sc.parallelize((1 to 5).map(i => OrderData(i, s"some $i", i)))
    val refDf = snSession.createDataFrame(rdd)
    snSession.sql("DROP TABLE IF EXISTS RR_TABLE")
    snSession.sql("DROP TABLE IF EXISTS COL_TABLE")

    snSession.sql("CREATE TABLE RR_TABLE(OrderRef INT NOT NULL, description String, price BIGINT)")
    snSession.sql("CREATE TABLE COL_TABLE(OrderRef INT NOT NULL, description String, price  LONG) using column options()")

    refDf.write.insertInto("RR_TABLE")
    refDf.write.insertInto("COL_TABLE")
  }

  def testUDFWithConnection(): Unit = {
    var snSession = new SnappySession(sc)
    createTables(snSession)

    val udfText: String = "public class IntegerUDF implements org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return 6; " +
        "}" +
        "}"
    val file = createUDFClass("IntegerUDF", udfText)
    val jar = createJarFile(Seq(file))

    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()

    s.execute(s"CREATE FUNCTION APP.intudf AS IntegerUDF " +
        s"RETURNS Integer USING JAR " +
        s"'$jar'")

    val row = snSession.sql("select intudf(description) from col_table").collect()
    row.foreach(r => println(r))
    row.foreach(r => assert(r(0) == 6))

    s.execute("drop function intudf")

    snSession = new SnappySession(sc)

    Try(snSession.sql("select intudf(description) from col_table ")) match {
      case Success(df) => throw new AssertionError(
        "Should not have succedded with dropped udf")
      case Failure(error) => // Do nothing
    }
    conn.close()
  }

  def testSameUDFWithCodeChange(): Unit = {
    val snSession = new SnappySession(sc)
    createTables(snSession)

    var udfText: String = "public class IntegerUDF implements org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return 6; " +
        "}" +
        "}"
    var file = createUDFClass("IntegerUDF", udfText)
    var jar = createJarFile(Seq(file))
    snSession.sql(s"CREATE FUNCTION APP.intudf AS IntegerUDF " +
        s"RETURNS Integer USING JAR " +
        s"'$jar'")
    var row = snSession.sql("select intudf(description) from col_table").collect()
    row.foreach(r => println(r))
    row.foreach(r => assert(r(0) == 6))

    udfText = "public class IntegerUDF implements org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return 7; " +
        "}" +
        "}"

    snSession.sql("drop function intudf")
    file = createUDFClass("IntegerUDF", udfText)
    jar = createJarFile(Seq(file))

    snSession.sql(s"CREATE FUNCTION APP.intudf AS IntegerUDF " +
        s"RETURNS Integer USING JAR " +
        s"'$jar'")

    row = snSession.sql("select intudf(description) from col_table").collect()
    row.foreach(r => println(r))
    row.foreach(r => assert(r(0) == 7))
    snSession.sql("drop function APP.intudf")
  }

  def testSameUDFWithFieldChange(): Unit = {
    val snSession = new SnappySession(sc)
    createTables(snSession)

    var udfText: String = "public class IntegerUDF implements org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        "\n                       " +
        " private int value = 6 ;" +
        " @Override public Integer call(String s){ " +
        "               return value; " +
        "}" +
        "}"
    var file = createUDFClass("IntegerUDF", udfText)
    var jar = createJarFile(Seq(file))
    snSession.sql(s"CREATE FUNCTION APP.intudf AS IntegerUDF " +
        s"RETURNS Integer USING JAR " +
        s"'$jar'")
    var row = snSession.sql("select intudf(description) from col_table").collect()
    row.foreach(r => println(r))
    row.foreach(r => assert(r(0) == 6))

    udfText = "public class IntegerUDF implements org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return 6; " +
        "}" +
        "}"

    snSession.sql("drop function intudf")
    file = createUDFClass("IntegerUDF", udfText)
    jar = createJarFile(Seq(file))

    snSession.sql(s"CREATE FUNCTION APP.intudf AS IntegerUDF " +
        s"RETURNS Integer USING JAR " +
        s"'$jar'")

    row = snSession.sql("select intudf(description) from col_table").collect()
    row.foreach(r => println(r))
    row.foreach(r => assert(r(0) == 6))
    snSession.sql("drop function APP.intudf")
  }

  def testTwoUDFsDroppingOne(): Unit = {
    val snSession = new SnappySession(sc)
    createTables(snSession)

    var udfText: String = "public class IntegerUDF1 implements org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return 6; " +
        "}" +
        "}"
    val file1 = createUDFClass("IntegerUDF1", udfText)

    udfText = "public class IntegerUDF2 implements org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return 8; " +
        "}" +
        "}"

    val file2 = createUDFClass("IntegerUDF2", udfText)

    val jar = createJarFile(Seq(file1,file2))
    snSession.sql(s"CREATE FUNCTION APP.intudf1 AS IntegerUDF1 " +
        s"RETURNS Integer USING JAR " +
        s"'$jar'")
    var row = snSession.sql("select intudf1(description) from col_table").collect()
    row.foreach(r => println(r))
    row.foreach(r => assert(r(0) == 6))

    snSession.sql(s"CREATE FUNCTION APP.intudf2 AS IntegerUDF2 " +
        s"RETURNS Integer USING JAR " +
        s"'$jar'")
    row = snSession.sql("select intudf2(description) from col_table").collect()
    row.foreach(r => println(r))
    row.foreach(r => assert(r(0) == 8))

    snSession.sql("drop function intudf1")

    row = snSession.sql("select intudf2(description) from col_table").collect()
    row.foreach(r => println(r))
    row.foreach(r => assert(r(0) == 8))
  }
}

object UserDefinedFunctionsDUnitTest {

  val userDir = System.getProperty("user.dir")

  val pathSeparator = File.pathSeparator

  def destDir : File ={
    val jarDir = new File(s"$userDir/jars")
    if(!jarDir.exists()){
      jarDir.mkdir()
    }
    jarDir
  }

  def getJavaSourceFromString(name: String, code: String): JavaSourceFromString = {
    new JavaSourceFromString(name, code)
  }

  def createUDFClass(name: String, code: String): File = {
    TestUtils.createCompiledClass(name, destDir, getJavaSourceFromString(name, code), Seq.empty[URL])
  }

  def createJarFile(files: Seq[File]): String = {
    val jarFile = new File(destDir, "testJar-%s.jar".format(System.currentTimeMillis()))
    TestUtils.createJar(files, jarFile)
    jarFile.getPath
  }
}
