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

import io.snappydata.cluster.ClusterManagerTestBase

import org.apache.spark.TestUtils
import org.apache.spark.TestUtils.JavaSourceFromString
import UserDefinedFunctionsDUnitTest._
import org.apache.spark.sql.SnappySession

case class OrderData(ref: Int, description: String, amount: Long)

class UserDefinedFunctionsDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) {

  def createTables(session: SnappySession) {
    val snSession = new SnappySession(sc)
    val rdd = sc.parallelize((1 to 5).map(i => OrderData(i, s"some $i", i)))
    val refDf = snSession.createDataFrame(rdd)
    snSession.sql("DROP TABLE IF EXISTS RR_TABLE")
    snSession.sql("DROP TABLE IF EXISTS COL_TABLE")

    snSession.sql("CREATE TABLE RR_TABLE(OrderRef INT NOT NULL, description String, price BIGINT)")
    snSession.sql("CREATE TABLE COL_TABLE(OrderRef INT NOT NULL, description String, price  LONG) using column options()")

    refDf.write.insertInto("RR_TABLE")
    refDf.write.insertInto("COL_TABLE")
  }

  def testJarDeployedWithSparkContext(): Unit = {
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
    var row = snSession.sqlUncached("select intudf(description) from col_table").collect()
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

    row = snSession.sqlUncached("select intudf(description) from col_table").collect()
    row.foreach(r => println(r))
    row.foreach(r => assert(r(0) == 7))

  }
}

object UserDefinedFunctionsDUnitTest {

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
}
