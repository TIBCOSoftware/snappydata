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
package org.apache.spark.sql.store


import java.math

import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.jdbc.{ConnectionConfBuilder, ConnectionUtil}
import org.apache.spark.sql.udf.UserDefinedFunctionsDUnitTest._

case class OrderData(ref: Int, description: String, price: Long,
    tax : BigDecimal, surcharge: Float, date: java.sql.Date, time : String)

class SnappyUDFTest extends SnappyFunSuite with BeforeAndAfterAll {



  override def beforeAll: Unit = {
    val rdd = sc.parallelize((1 to 5).map(i => OrderData(i, s"some $i", i, i/2,
      i/2 , java.sql.Date.valueOf("2012-12-12"), "2000-02-03 12:23:04")))
    val refDf = snc.createDataFrame(rdd)
    refDf.createTempView("tempTable")

    snc.sql("DROP TABLE IF EXISTS RR_TABLE")
    snc.sql("DROP TABLE IF EXISTS COL_TABLE")

    snc.sql("CREATE TABLE RR_TABLE(OrderRef INT NOT NULL, description String, " +
        "price BIGINT, serviceTax DECIMAL, surcharge Float, purchase_date DATE, time Timestamp)")

    snc.sql("CREATE TABLE COL_TABLE(OrderRef INT NOT NULL, description String, price  " +
        "LONG, serviceTax DECIMAL, surcharge Float, purchase_date DATE, time Timestamp) " +
        "using column options(PARTITION_BY 'OrderRef')")

    refDf.write.insertInto("RR_TABLE")
    refDf.write.insertInto("COL_TABLE")
  }

  override def afterAll: Unit = {
    snc.sql("DROP TABLE IF EXISTS RR_TABLE")
    snc.sql("DROP TABLE IF EXISTS COL_TABLE")
  }

  private def dropUdf(udfName: String): Unit = {
    snc.sql(s"drop function $udfName")
    snc.sql(s"drop function if exists $udfName")
  }

  private def showDescribe(udfName : String): Unit = {
    assert(snc.snappySession.sessionCatalog.listFunctions("app",
      s"${udfName.substring(0, udfName.length - 2)}*").
      find(f => (f._1.toString().contains(udfName))).size == 1)

    assert(snc.snappySession.sql(s"DESCRIBE FUNCTION $udfName").collect().length == 3)
    assert(snc.snappySession.sql(s"DESCRIBE FUNCTION EXTENDED $udfName").collect().length == 4)
    assert(snc.snappySession.sql(s"DESCRIBE FUNCTION $udfName").collect().length == 3)
    assert(snc.snappySession.sql(s"DESCRIBE FUNCTION EXTENDED $udfName").collect().length == 4)
    assert(snc.snappySession.sql(s"SHOW FUNCTIONS $udfName").collect().length == 1)
    assert(snc.snappySession.sql(s"SHOW FUNCTIONS $udfName").collect().length == 1)
  }

  test("Test UDF with Byte  Return type with schema") {
    val udfText: String = "public class ByteUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<java.lang.String, java.lang.Byte> {" +
      " @Override public java.lang.Byte call(String s){ " +
      "               return new java.lang.Byte((byte)122); " +
      "}" +
      "}"
    val file = createUDFClass("ByteUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.byteudf AS ByteUDF " +
      s"RETURNS BYTE USING JAR " +
      s"'$jar'")
    snc.sql("select app.byteudf(description) from col_table a").collect()
    snc.sql("select APP.byteudf(description) from rr_table").collect()
    showDescribe("byteudf")
    dropUdf("byteudf")
  }

  test("Test Nested UDF with schema") {
    var udfText: String = "public class NestedUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<java.lang.String, java.lang.String> {" +
      " @Override public java.lang.String call(String s){ " +
      "               return s; " +
      "}" +
      "}"
    var file = createUDFClass("NestedUDF", udfText)
    var jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.nestedudf AS NestedUDF " +
      s"RETURNS STRING USING JAR " +
      s"'$jar'")

    udfText = "public class SubUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<java.lang.String, java.lang.String> {" +
      " @Override public java.lang.String call(String s){ " +
      "               return s; " +
      "}" +
      "}"
    file = createUDFClass("SubUDF", udfText)
    jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.subudf AS SubUDF " +
      s"RETURNS STRING USING JAR " +
      s"'$jar'")

    snc.sql(s"""select app.SubUDF(nvl(description,'some')) from col_table""").collect()
    snc.sql("select    app.SubUDF(nvl(description,'some')) from rr_table").collect()
    snc.sql(s"""select app.SubUDF(app.nestedudf(description)) from col_table""").collect()
    snc.sql("select    app.SubUDF(app.nestedudf(description)) from rr_table").collect()

    snc.sql("select {FN app.SubUDF(nvl(description,'some')) } from col_table a").collect()
    snc.sql("select {FN app.SubUDF(nvl(description,'some')) } from rr_table").collect()
    snc.sql("select {FN app.SubUDF(app.nestedudf(description)) } from col_table a").collect()
    snc.sql("select {FN app.SubUDF(app.nestedudf(description)) } from rr_table").collect()

    dropUdf("nestedudf")
    dropUdf("subudf")
  }

  test("Test Count(*) ") {
    snc.sql("select count(*) from col_table a").collect()
    snc.sql("select count(*) from rr_table").collect()
  }

  test("Test UDF fn syntax without schema") {
    val udfText: String = "public class ByteUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<java.lang.String, java.lang.Byte> {" +
      " @Override public java.lang.Byte call(String s){ " +
      "               return new java.lang.Byte((byte)122); " +
      "}" +
      "}"
    val file = createUDFClass("ByteUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.byteudf AS ByteUDF " +
      s"RETURNS BYTE USING JAR " +
      s"'$jar'")
    snc.sql("select description, {fn byteudf(description) } from col_table a").collect()
    snc.sql("select description, {fn byteudf(description) } from rr_table").collect()
    showDescribe("byteudf")
    dropUdf("byteudf")
  }

  test("Test UDF fn syntax with schema") {
    val udfText: String = "public class ByteUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<java.lang.String, java.lang.Byte> {" +
      " @Override public java.lang.Byte call(String s){ " +
      "               return new java.lang.Byte((byte)122); " +
      "}" +
      "}"
    val file = createUDFClass("ByteUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.byteudf AS ByteUDF " +
      s"RETURNS BYTE USING JAR " +
      s"'$jar'")
    snc.sql("select {FN app.byteudf(description) } from col_table a").collect()
    snc.sql("select {FN APP.byteudf(description) } from rr_table").collect()
    showDescribe("byteudf")
    dropUdf("byteudf")
  }

  test("Test UDF with Byte  Return type") {
    val udfText: String = "public class ByteUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<java.lang.String, java.lang.Byte> {" +
        " @Override public java.lang.Byte call(String s){ " +
        "               return new java.lang.Byte((byte)122); " +
        "}" +
        "}"
    val file = createUDFClass("ByteUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.byteudf AS ByteUDF " +
        s"RETURNS BYTE USING JAR " +
        s"'$jar'")
    snc.sql("select byteudf(description) from col_table").collect()
    snc.sql("select byteudf(description) from rr_table").collect()
    showDescribe("byteudf")
    dropUdf("byteudf")
  }

  test("Test UDF with Short  Return type") {
    val udfText: String = "public class ShortUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<java.lang.String, java.lang.Short> {" +
        " @Override public java.lang.Short call(String s){ " +
        "               return new java.lang.Short((short)122); " +
        "}" +
        "}"
    val file = createUDFClass("ShortUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.shortudf AS ShortUDF " +
        s"RETURNS SHORT USING JAR " +
        s"'$jar'")
    snc.sql("select shortudf(description) from col_table").collect()
    snc.sql("select shortudf(description) from rr_table").collect()
    showDescribe("shortudf")
    dropUdf("shortudf")
  }

  test("Test UDF with TIMESTAMP  Return type") {
    val udfText: String = "public class TimeUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<java.sql.Timestamp, java.sql.Timestamp> {" +
        " @Override public java.sql.Timestamp call(java.sql.Timestamp s){ " +
        "               return s; " +
        "}" +
        "}"
    val file = createUDFClass("TimeUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.timeudf AS TimeUDF " +
        s"RETURNS Timestamp USING JAR " +
        s"'$jar'")
    snc.sql("select timeudf(time) from col_table").collect()
    snc.sql("select timeudf(time) from rr_table").collect()
    showDescribe("timeudf")
    dropUdf("timeudf")
  }

  test("Test UDF with Double  Return type") {
    val udfText: String = "public class DoubleUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<java.lang.String, java.lang.Double> {" +
        " @Override public java.lang.Double call(String s){ " +
        "               return new java.lang.Double(12223.678); " +
        "}" +
        "}"
    val file = createUDFClass("DoubleUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.doubleudf AS DoubleUDF " +
        s"RETURNS Double USING JAR " +
        s"'$jar'")
    snc.sql("select doubleudf(description) from col_table").collect()
    snc.sql("select doubleudf(description) from rr_table").collect()
    showDescribe("doubleudf")
    dropUdf("doubleudf")
  }

  test("Test UDF with Boolean  Return type") {
    val udfText: String = "public class BooleanUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<java.lang.String, java.lang.Boolean> {" +
        " @Override public java.lang.Boolean call(String s){ " +
        "               return new java.lang.Boolean(true); " +
        "}" +
        "}"
    val file = createUDFClass("BooleanUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.booludf AS BooleanUDF " +
        s"RETURNS Boolean USING JAR " +
        s"'$jar'")
    snc.sql("select booludf(description) from col_table").collect()
    snc.sql("select booludf(description) from rr_table").collect()
    dropUdf("booludf")
  }

  test("Test UDF with Date  Return type") {
    val udfText: String = "public class DateUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<java.sql.Date, java.sql.Date> {" +
        " @Override public java.sql.Date call(java.sql.Date s){ " +
        "               return s; " +
        "}" +
        "}"
    val file = createUDFClass("DateUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.dateudf AS DateUDF " +
        s"RETURNS Date USING JAR " +
        s"'$jar'")
    snc.sql("select dateudf(purchase_date) from col_table").collect()
    snc.sql("select dateudf(purchase_date) from rr_table").collect()
    dropUdf("dateudf")
  }

  test("Test UDF with float  Return type") {
    // Intentionally used double types for row tables
    val udfText: String = "public class FloatUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<Float, Float> {" +
        " @Override public Float call(Float s){ " +
        "               return s; " +
        "}" +
        "}"

    val udfText1: String = "public class DoubleUDF1 implements" +
      " org.apache.spark.sql.api.java.UDF1<Double, Double> {" +
        " @Override public Double call(Double s){ " +
        "               return s; " +
        "}" +
        "}"
    val file1 = createUDFClass("FloatUDF", udfText)
    val file2 = createUDFClass("DoubleUDF1", udfText1)
    val jar = createJarFile(Seq(file1, file2))

    snc.sql(s"CREATE FUNCTION APP.floatudf AS FloatUDF " +
        s"RETURNS Float USING JAR " +
        s"'$jar'")

    snc.sql(s"CREATE FUNCTION APP.doubleudf1 AS DoubleUDF1 " +
        s"RETURNS Double USING JAR " +
        s"'$jar'")
    snc.sql("select floatudf(surcharge) from col_table").collect()
    snc.sql("select doubleudf1(surcharge) from rr_table").collect()
    dropUdf("floatudf")
    dropUdf("doubleudf1")
  }


  test("Test UDF with decimal  Return type") {
    val udfText: String = "public class DecimalUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<java.math.BigDecimal, java.math.BigDecimal> {" +
        " @Override public java.math.BigDecimal call(java.math.BigDecimal s){ " +
        "               return s; " +
        "}" +
        "}"
    val file = createUDFClass("DecimalUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.decimaludf AS DecimalUDF " +
        s"RETURNS DECIMAL USING JAR " +
        s"'$jar'")
    snc.sql("select decimaludf(serviceTax) from col_table").collect()
    snc.sql("select decimaludf(serviceTax) from rr_table").collect()
    dropUdf("decimaludf")
  }

  test("Test UDF with Integer  Return type") {
    val udfText: String = "public class IntegerUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return s.length(); " +
        "}" +
        "}"
    val file = createUDFClass("IntegerUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.intudf AS IntegerUDF " +
        s"RETURNS Integer USING JAR " +
        s"'$jar'")
    snc.sql("select intudf(description) from col_table").collect()
    snc.sql("select intudf(description) from rr_table").collect()
    dropUdf("intudf")
  }

  test("Test UDF with Long  Return type") {
    val udfText: String = "public class LongUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<Long,Long> {" +
        " @Override public Long call(Long s){ " +
        "               return s; " +
        "}" +
        "}"
    val file = createUDFClass("LongUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.longudf AS LongUDF " +
        s"RETURNS Long USING JAR " +
        s"'$jar'")
    snc.sql("select longudf(PRICE) from col_table").collect()
    snc.sql("select longudf(PRICE) from rr_table").collect()
    dropUdf("longudf")
  }


  test("Test UDF with Multiple  interface") {
    val udfText: String = "public class MultUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<Integer,Integer>," +
      " org.apache.spark.sql.api.java.UDF2<Integer,Integer, Integer> {" +
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
        s"RETURNS Integer USING JAR " +
        s"'$jar'")
    snc.sql("select multudf(OrderRef) from col_table").collect()
    snc.sql("select multudf(OrderRef, OrderRef) from col_table")

    snc.sql("select multudf(OrderRef) from rr_table").collect()
    snc.sql("select multudf(OrderRef, OrderRef) from rr_table").collect()
    dropUdf("multudf")
  }


  test("Test UDAFs") {

    val udafTest : String = "import org.apache.spark.sql.Row;" +
        "import org.apache.spark.sql.expressions.MutableAggregationBuffer;" +
        "import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;" +
        "import org.apache.spark.sql.types.DataType;" +
        "import org.apache.spark.sql.types.DataTypes;" +
        "import org.apache.spark.sql.types.StructType;" +
        "" +
        "public class LongProductSum extends UserDefinedAggregateFunction " +
        "{  " +
        "public StructType inputSchema() {" +
        "    return new StructType()" +
        "        .add(\"a\", DataTypes.LongType)" +
        "        .add(\"b\", DataTypes.LongType);" +
        "  }" +
        "  " +
        "  public StructType bufferSchema() {" +
        "    return new StructType()" +
        "        .add(\"product\", DataTypes.LongType);" +
        "  }" +
        "  public DataType dataType() {" +
        "    return DataTypes.LongType;" +
        "  }" +
        "  public boolean deterministic() {" +
        "    return true;" +
        "  }" +
        "  public void initialize(MutableAggregationBuffer buffer) {" +
        "    buffer.update(0, 0L);" +
        "  }" +
        "  public void update(MutableAggregationBuffer buffer, Row input) {" +
        "    if (!(input.isNullAt(0) || input.isNullAt(1))) {" +
        "      buffer.update(0, buffer.getLong(0) + input.getLong(0) * input.getLong(1));" +
        "    }" +
        "  }" +
        "  public void merge(MutableAggregationBuffer buffer1, Row buffer2) {" +
        "    buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0));" +
        "  }" +
        "  public Object evaluate(Row buffer) {" +
        "    return buffer.getLong(0);" +
        "  }" +
        "}"
    val file = createUDFClass("LongProductSum", udafTest)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.longproductsum AS LongProductSum " +
        s" RETURNS LONG USING JAR " +
        s"'$jar'")
    snc.sql("select longproductsum(price, price) from col_table").collect()
    dropUdf("longproductsum")
  }

  test("Test UDF with String  Return type") {
    val udfText: String = "public class StringUDF implements" +
      " org.apache.spark.sql.api.java.UDF1<String,String> {" +
        " @Override public String call(String s){ " +
        "               return s + s; " +
        "}" +
        "}"
    val file = createUDFClass("StringUDF", udfText)
    val jar = createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.strudf AS StringUDF " +
        s"RETURNS STRING USING JAR " +
        s"'$jar'")
    snc.sql("select strudf(description) from col_table").collect()
    snc.sql("select strudf(description) from rr_table").collect()
    dropUdf("strudf")
  }

  test("Test Spark UDF") {
    snc.udf.register("decudf", (n: java.math.BigDecimal) => { n.multiply(new math.BigDecimal(2)) })
    snc.sql("select decudf(tax) from tempTable").collect()
  }



  test("test dsid function") {

   snc.sql("create table test123( a integer,b integer, c int) using column options()")
    snc.sql("insert into test123 values(1,2,3)")
    snc.sql("insert into test123 values(31,42,53)")
    snc.sql("insert into test123 values(87,76,63)")
    snc.sql("insert into test123 values(12,24,53)")

    snc.sql("select DSID() from test123").collect().foreach(row => {
      assert(row.getString(0).equals(Misc.getMyId().getId()))
    });
    snc.sql("drop table test123")
  }

  test("Test UDF other schema") {
    val conf = new ConnectionConfBuilder(snc.snappySession).build()
    val conn = ConnectionUtil.getPooledConnection("test default conf", conf)
    try{
      val st = conn.createStatement
      st.execute("create schema trade")
      val udfText: String = "public class StringUDF implements" +
          " org.apache.spark.sql.api.java.UDF1<String,String> {" +
          " @Override public String call(String s){ " +
          "               return s + s; " +
          "}" +
          "}"
      val file = createUDFClass("StringUDF", udfText)
      val jar = createJarFile(Seq(file))

      snc.sql(s"CREATE FUNCTION TRADE.STRUDF AS StringUDF " +
          s"RETURNS STRING USING JAR " +
          s"'$jar'")

      snc.sql("CREATE TABLE trade.rr_test_table(OrderRef INT NOT NULL, description String, " +
          "price BIGINT, serviceTax DECIMAL, surcharge Float, purchase_date DATE, time Timestamp)")

      snc.sql("CREATE TABLE trade.col_test_table(OrderRef INT NOT NULL," +
          " description String, price  " +
          "LONG, serviceTax DECIMAL, surcharge Float, purchase_date DATE, time Timestamp) " +
          "using column options(PARTITION_BY 'OrderRef')")


      snc.sql("select TRADE.strudf(description) from trade.col_test_table").collect()
      snc.sql("select trade.STRUDF(description) from trade.rr_test_table").collect()
      snc.sql("select TRADE.STRUDF(description) from trade.rr_test_table").collect()
      dropUdf("trade.strudf")
    } finally {
      snc.sql("DROP TABLE IF EXISTS trade.col_test_table")
      snc.sql("DROP TABLE IF EXISTS trade.rr_test_table")
    }

  }
}
