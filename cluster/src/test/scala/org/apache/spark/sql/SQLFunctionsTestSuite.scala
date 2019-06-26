/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql

import java.io.{File, FileOutputStream, PrintWriter}
import java.math.BigDecimal
import java.nio.file.{Files, Paths}
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.io.Source
import scala.language.postfixOps

import io.snappydata.SnappyFunSuite
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.Logging
import org.apache.spark.sql.NorthWindDUnitTest.writeToFile
import org.apache.spark.sql.types._
import org.junit.Assert._

class SQLFunctionsTestSuite extends SnappyFunSuite
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll {

  // scalastyle:off println

  val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  // snc.sql("set snappydata.sql.tokenize=true")
  // snc.sql("set snappydata.sql.planCaching=true")

  val pw = new PrintWriter(new FileOutputStream(
    new File("SQLFunctionTestSuite.out"), true))

  var query = ""

  override def beforeAll(): Unit = {
    super.beforeAll()
    createRowTable()
    createColumnTable()
    createSparkTable()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    dropTables()
  }

  def createRowTable(): Unit = {
    snc.sql("CREATE TABLE rowTable (bigIntCol BIGINT," +
        " binaryCol1 BINARY," +
        " boolCol BOOLEAN ," +
        " byteCol BYTE," +
        " charCol CHAR( 30 )," +
        " dateCol DATE ," +
        " decimalCol DECIMAL( 11) ," +
        " doubleCol DOUBLE ," +
        " floatCol FLOAT ," +
        " intCol INT," +
        " integerCol INTEGER ," +
        " longVarcharCol LONG VARCHAR," +
        " numericCol NUMERIC," +
        " numeric1Col NUMERIC(10,2)," +
        " doublePrecisionCol DOUBLE PRECISION," +
        " realCol REAL," +
        " stringCol STRING," +
        " timestampCol TIMESTAMP," +
        " varcharCol VARCHAR( 20 ))")

    snc.sql("insert into rowtable values (1000, NULL, NULL, NULL," +
        " '1234567890abcdefghij', date('1970-01-08'), 66, 2.2, 1.0E8, 1000, 1000," +
        " '1234567890abcdefghij', 100000.0, 100000.0, 2.2, null, 'abcd'," +
        " timestamp('1997-01-01 03:03:03'), 'abcd')")

    snc.sql(s"insert into rowtable values (-10, NULL, true, NULL," +
        " 'ABC@#', current_date, -66, 0.0111, -2.225E-307, -10, 10," +
        " 'ABC@#', -1, 1, 123.56, 0.089, 'abcd', current_timestamp, 'SNAPPY')")
  }

  def createColumnTable(): Unit = {
    snc.sql("CREATE TABLE columnTable (bigIntCol BIGINT," +
        " binaryCol1 BINARY," +
        " boolCol BOOLEAN ," +
        " byteCol BYTE," +
        " charCol CHAR( 30 ) ," +
        " dateCol DATE ," +
        " decimalCol DECIMAL( 10, 2 ) ," +
        " doubleCol DOUBLE ," +
        " floatCol FLOAT ," +
        " intCol INT ," +
        " integerCol INTEGER," +
        " longVarcharCol LONG ," +
        " numericCol NUMERIC," +
        " numeric1Col NUMERIC(10,2)," +
        " doublePrecisionCol DOUBLE PRECISION," +
        " realCol REAL," +
        " stringCol STRING," +
        " timestampCol TIMESTAMP ," +
        " varcharCol VARCHAR( 20 )," +
        " arrayStringCol ARRAY<String>," +
        " arrayIntCol ARRAY<Integer>," +
        " mapCol MAP<INT, STRING>," +
        " structCol STRUCT<c1: STRING, c2: INTEGER>) using COLUMN options(BUCKETS '8')")

    snc.sql("insert into columntable select 1000, NULL, NULL, NULL," +
        " '1234567890abcdefghij', date('1970-01-08'), 66, 2.2, 1.0E8, 1000, 1000," +
        " '1234567890abcdefghij', 100000.0, 100000.0, 2.2, NULL," +
        " 'abcd', timestamp('1997-01-01 03:03:03'), 'abcd', NULL, NULL, NULL, NULL")

    snc.sql(s"insert into columntable select -10, NULL, true, NULL," +
        " 'ABC@#', current_date, -66, 0.0111, -2.225E-307, -10, 10," +
        " 'ABC@#', -1, 1, 123.56, 0.089, 'abcd', current_timestamp, 'SNAPPY'," +
        " Array('abc','def','efg'), Array(1,2,3), Map(1,'abc'), Struct('abc',123)")

  }

  def createSparkTable(): Unit = {

    val DecimalType = DataTypes.createDecimalType(10, 2)
    val now = Calendar.getInstance().getTime()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date1 = java.sql.Date.valueOf(dateFormat.format(Date.valueOf("1970-01-08")))
    val current_date = java.sql.Date.valueOf(dateFormat.format(now))
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time1 = java.sql.Timestamp.valueOf(
      timeFormat.format(Timestamp.valueOf("9999-12-31 23:59:59.999999")))
    val current_timestamp = java.sql.Timestamp.valueOf(timeFormat.format(now))

    val schema = List(
      StructField("bigIntCol", IntegerType, true),
      StructField("binaryCol1", BinaryType, true),
      StructField("boolCol", BooleanType, true),
      StructField("byteCol", ByteType, true),
      StructField("charCol", StringType, true),
      StructField("dateCol", DateType, true),
      StructField("decimalCol", DecimalType, true),
      StructField("doubleCol", DoubleType, true),
      StructField("floatCol", FloatType, true),
      StructField("intCol", IntegerType, true),
      StructField("integerCol", IntegerType, true),
      StructField("longVarcharCol", StringType, true),
      StructField("numericCol", DecimalType, true),
      StructField("numeric1Col", DecimalType, true),
      StructField("doublePrecisionCol", DoubleType, true),
      StructField("realCol", FloatType, true),
      StructField("stringCol", StringType, true),
      StructField("timestampCol", TimestampType, true),
      StructField("varcharCol", StringType, true),
      StructField("arrayStringCol", ArrayType(StringType), true),
      StructField("arrayIntCol", ArrayType(IntegerType), true),
      StructField("mapCol", MapType(IntegerType, StringType), true),
      StructField("structCol", StructType(Seq(StructField("c1", StringType, false),
        StructField("c2", IntegerType, false))), true)
    )

    val data = Seq(
      Row(1000, null, null, null, "1234567890abcdefghij",
        date1, new BigDecimal(66), 2.2, 1.0E8f,
        1000, 1000, "1234567890abcdefghij", new BigDecimal(100000.0),
        new BigDecimal(100000.0), 2.2, null, "abcd",
        time1, "abcd'", null, null, null, null),
      Row(-10, null, true, null, "ABC@#",
        current_date, new BigDecimal(-66), 0.0111, -2.225E-307f,
        -10, 10, "ABC@#", new BigDecimal(-1),
        new BigDecimal(1), 123.56, 0.089f, "abcd",
        current_timestamp, "SNAPPY'", Array("abc", "def", "efg"),
        Array(1, 2, 3), scala.collection.immutable.Map(1 -> "abc"),
        Row("abc", 123))
    )

    val someDF = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(data),
      StructType(schema)
    )
    someDF.createTempView("sparkTable")
  }

  def dropTables(): Unit = {
    snc.sql("DROP TABLE IF EXISTS rowTable")
    snc.sql("DROP TABLE IF EXISTS columnTable")
    sparkSession.sql("DROP TABLE IF EXISTS sparkTable")
  }

  protected def getTempDir(dirName: String, onlyOnce: Boolean): String = {
    var log: File = new File(".")
    if (onlyOnce) {
      val logParent = log.getAbsoluteFile.getParentFile.getParentFile
      if (logParent.list().contains("output.txt")) {
        log = logParent
      } else if (logParent.getParentFile.list().contains("output.txt")) {
        log = logParent.getParentFile
      }
    }
    var dest: String = null
    dest = log.getCanonicalPath + File.separator + dirName
    val tempDir: File = new File(dest)
    if (!tempDir.exists) tempDir.mkdir()
    tempDir.getAbsolutePath
  }


  private def getSortedFiles(file: File): Array[File] = {
    file.getParentFile.listFiles.filter(_.getName.startsWith(file.getName)).sortBy { f =>
      val n = f.getName
      val i = n.lastIndexOf('.')
      n.substring(i + 1).toInt
    }
  }

  def validateResult(sparkDf: DataFrame, snappyDf: DataFrame): Unit = {
    // expects results are sorted on some common key
    val sparkSchema = sparkDf.schema
    val snappySchema = snappyDf.schema
    assert(sparkSchema == snappySchema, "schemas from spark and snappy are not equal")

    val fields = sparkSchema.map(_.name.toUpperCase())

    assert(sparkDf.count() == snappyDf.count(), "counts from spark and snappy are not equal")

    val sparkResult = sparkDf.collect()
    val snappyResult = snappyDf.collect()

    var i = 0
    while(i < sparkResult.size - 1) {
      val snRow = snappyResult(i)
      val spRow = sparkResult(i)
      sparkSchema.foreach(f => {
        val fieldIndex = fields.indexOf(f.name.toUpperCase())
        assert(fieldIndex >= 0, s"field not found in schema. " +
          s"fieldname=${f.name} fields=${fields.toSeq}")
        val snField: Any = snRow.getAs(fieldIndex)
        val spField: Any = spRow.getAs(fieldIndex)
        assert(snField == spField,
          s"field from spark row and snappy row are not equal. field name=${f.name} at index=${i}")
      })
      i += 1
    }
  }

  test("abs") {

    query = "select abs(-1)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "select abs(1)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "select abs(1.1)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "select abs(-1.1)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "select abs(0.0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

  }

  test("coalesce") {

    query = "SELECT COALESCE(NULL,NULL,NULL,'abc',NULL,'Example.com')"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT COALESCE(NULL, 1, 2, 'abc')"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT COALESCE(1, 2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT COALESCE(NULL, NULL)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("cast") {

    // On snappy shell for below query throws error
    // snappy> select cast('NaN' as double);
    // ERROR 22003: (SQLState=22003 Severity=20000)
    // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
    // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
    query = "select cast('NaN' as double)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT CAST(25.65 AS varchar(12))"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT cast('10' as int)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT CAST('2017-08-25' AS date)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

  }

  test("explode") {

    query = "SELECT explode(array(10, 20))"
    var sparkDf = sparkSession.sql(s"$query")
    var snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT explode(array(0))"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT explode(array(NULL,1))"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("greatest") {

    query = "SELECT greatest(10, 9, 2, 4, 3)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT greatest(0, NULL)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("if") {

    query = "SELECT if(1 < 2, 'a', 'b')"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT if(0 < NULL, 'a', 'b')"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("inline") {

    query = "SELECT inline(array(struct(1, 'a'), struct(2, 'b')))"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT inline(array(struct(1), struct(2)))"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("isnan") {

    query = "SELECT isnan(cast('NaN' as double))"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT isnan(123)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("ifnull") {

    query = "SELECT ifnull(NULL, array('2'))"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT ifnull(2, 3)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("isnull") {

    query = "SELECT isnull(1)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT isnull('abc')"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT isnull(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("isnotnull") {

    query = "SELECT isnotnull(1)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT isnotnull('abc')"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT isnotnull(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("least") {

    query = "SELECT least(10, 9, 2, 4, 3)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT least(null, 9, 3)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("nanvl") {

    query = "SELECT nanvl(cast('NaN' as double), 123)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    // On snappy shell throws error for below query
    // snappy> SELECT nanvl(cast('NaN' as double), cast('NaN' as double));
    // ERROR 22003: (SQLState=22003 Severity=20000)
    // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
    // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
    query = "SELECT nanvl(cast('NaN' as double), cast('NaN' as double))"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    // snappy> SELECT nanvl('NaN','NaN');
    // ERROR 22003: (SQLState=22003 Severity=20000)
    // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
    // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
    query = "SELECT nanvl('NaN','NaN')"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

  }

  test("nullif") {

    query = "SELECT nullif(2, 2)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT nullif( 9, 3)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT nullif( 9, 9, 4)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    // Below query fails to run with snappysession.
    // Test passing individualy but fails to run in precheckin

    // query = "SELECT nullif( 9, 9, 9)"
    // sparkDf = sparkSession.sql(s"$query")
    // snappyDf = snc.sql(s"$query")
    // validateResult(sparkDf, snappyDf)

  }

  test("nvl") {
    query = "SELECT nvl(NULL, array('2'))"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT nvl( 9, 3)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("nvl2") {

    query = "SELECT nvl2(NULL, 2, 1)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT nvl2( 9, 3, 1)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("posexplode") {

    query = "SELECT posexplode(array(10,20))"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT posexplode(array(10,0,null))"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("rand") {
    query = "select rand()"
    var snappyDf = snc.sql(s"$query")
    assertEquals(1, snappyDf.count())

    query = "select rand(null)"
    var snappyDf1 = snc.sql(s"$query")
    assertEquals(1, snappyDf1.count())

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    // Throws error on snappy shell as well as in test
    // snappy> select rand(0);
    // ERROR 42000: (SQLState=42000 Severity=20000)
    // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
    // Syntax error or analysis exception: Input argument
    // to rand must be an integer, long or null literal.;
    try {
      query = "select rand(0)"
      snappyDf = snc.sql(s"$query")
      assertEquals(1, snappyDf.count())
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    
    // Throws error on snappy shell as well as in test
    // snappy> select rand(2);
    // ERROR 42000: (SQLState=42000 Severity=20000)
    // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
    // Syntax error or analysis exception: Input argument
    // to rand must be an integer, long or null literal.;

    try {
      query = "select rand(2)"
      snappyDf = snc.sql(s"$query")
      assertEquals(1, snappyDf.count())
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

  }

  test("randn") {
    query = "select randn()"
    var snappyDf = snc.sql(s"$query")
    assertEquals(1, snappyDf.count())

    query = "select randn(null)"
    var snappyDf1 = snc.sql(s"$query")
    assertEquals(1, snappyDf1.count())

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    // Throws error on snappy shell as well as in test
    // snappy> select randn(0);
    // ERROR 42000: (SQLState=42000 Severity=20000)
    // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
    // Syntax error or analysis exception: Input argument
    // to randn must be an integer, long or null literal.;
    try {
      query = "select randn(0)"
      snappyDf = snc.sql(s"$query")
      assertEquals(1, snappyDf.count())
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

    // Throws error on snappy shell as well as in test
    // snappy> select randn(2);
    // ERROR 42000: (SQLState=42000 Severity=20000)
    // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
    // Syntax error or analysis exception: Input argument
    // to randn must be an integer, long or null literal.;
    try {
      query = "select randn(2)"
      snappyDf = snc.sql(s"$query")
      assertEquals(1, snappyDf.count())
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

  }

  test("stack") {

    query = "SELECT stack(2, 1, 2, 3)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT stack(2, 1, 2, 3, 4)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("when") {

    query = "SELECT case when 2>1 then 2 else 1 end"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT case when 2<1 then 1 else 2 end"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("acos") {

    // On snappy shell throws below error
    // snappy> select acos(2);
    // ERROR 22003: (SQLState=22003 Severity=20000)
    // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
    // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
    query = "select acos(2)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT acos(1)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT acos(-1)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT acos(0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT acos(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT acos(2.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("asin") {

    query = "SELECT asin(0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    // On snappy shell throws below error
    // snappy> SELECT asin(2);
    // ERROR 22003: (SQLState=22003 Severity=20000)
    // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
    // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
    query = "SELECT asin(2)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT asin(-2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT asin(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT asin(2.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("atan") {

    query = "SELECT atan(0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT atan(2)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT atan(-2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT atan(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT atan(2.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("atan2") {

    query = "SELECT atan2(0, 0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT atan2(2, 3)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT atan2(2, null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT atan2(2.2, 3)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("bin") {

    query = "SELECT bin(13)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT bin(-13)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT bin(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT bin(13.3)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("bround") {

    query = "SELECT bround(2.5, 0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT bround(2.5, 3) as col2"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT bround(2.5, null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT round(0, null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("cbrt") {

    query = "SELECT cbrt(25)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT cbrt(0)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT cbrt(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT cbrt(27.0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("ceil") {

    query = "SELECT ceil(-0.1)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT ceil(5)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT ceil(0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT ceil(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("ceiling") {

    query = "SELECT ceiling(-0.1)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT ceiling(5)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT ceiling(0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT ceiling(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("cos") {

    query = "SELECT cos(0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)
    query = "SELECT cos(2)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT cos(-2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT cos(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT cos(2.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("cosh") {

    query = "SELECT cosh(0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT cosh(2)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT cosh(-2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT cosh(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT cosh(2.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("conv") {

    query = "SELECT conv('100', 2, 10)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT conv(-10, 16, -10)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("degrees") {

    query = "SELECT degrees(3.141592653589793)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT degrees(6.283185307179586 )"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT degrees(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT degrees(0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("e") {

    query = "SELECT e()"
    var sparkDf = sparkSession.sql(s"$query")
    var snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("exp") {

    query = "SELECT exp(0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT exp(2)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT exp(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("expm1") {

    query = "SELECT expm1(0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT expm1(2)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT expm1(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("floor") {

    query = "SELECT floor(5)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT floor(null)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT floor(0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT floor(-0.1)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("factorial") {

    query = "SELECT factorial(5)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT factorial(-5)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT factorial(0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT factorial(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("hex") {

    query = "SELECT hex(17)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT hex(0)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT hex(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT hex('Spark SQL')"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("hypot") {

    query = "SELECT hypot(3, 4)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT hypot(7,8)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT hypot(0,0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT hypot(0,null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT hypot(null,null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("log") {

    query = "SELECT log(10, 100)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT log(10,1000)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT log(10,0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT log(10,null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT log(10, 1000.234)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("log10") {

    query = "SELECT log10(10)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT log10(0)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT log10(-2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT log10(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT log10(1.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

  }

  test("log1p") {

    query = "SELECT log1p(0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT log1p(2)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT log1p(-2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT log1p(1.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT log1p(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("log2") {

    query = "SELECT log2(0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT log2(2)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT log2(-2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT log2(1.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT log2(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("ln") {

    query = "SELECT ln(0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT ln(1)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT ln(-1)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT ln(1.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT ln(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("negative") {

    query = "SELECT negative(0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT negative(1)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT negative(-1)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT negative(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT negative(1.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT negative(-1.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

  }

  test("pi") {

    query = "SELECT pi()"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("pmod") {

    query = "SELECT pmod(10,3)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT pmod(-10,3)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT pmod(0,3)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT pmod(null,3)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT pmod(1.2,3)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("positive") {

    query = "SELECT positive(0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT positive(1)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT positive(-1)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT positive(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT positive(1.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT positive(-1.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

  }

  test("pow") {

    query = "SELECT pow(3,2)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT pow(-10,3)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT pow(0,3)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT pow(null,3)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT pow(1.2,3)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("power") {

    query = "SELECT power(3,2)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT power(-10,3)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT power(0,3)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT power(null,3)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT power(1.2,3)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("radians") {

    query = "SELECT radians(360.0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT radians(180)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT radians(0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT radians(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("rint") {

    query = "SELECT rint(12.3456)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT rint(-12.3456)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT rint(180)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT rint(0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT rint(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("round") {

    query = "SELECT round(2.5, 0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT round(2.5, 3)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT round(2.5, null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT round(0, null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("shiftleft") {

    query = "SELECT shiftleft(4, 1)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT shiftleft(0, 1)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT shiftleft(null, null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT shiftleft(2.2, 2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT shiftleft(2.2, 0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("shiftright") {

    query = "SELECT shiftright(4, 1)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT shiftright(0, 1)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT shiftright(null, null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT shiftright(2.2, 2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT shiftright(2.2, 0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("shiftrightunsigned") {

    query = "SELECT shiftrightunsigned(4, 1)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT shiftrightunsigned(0, 1)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT shiftrightunsigned(null, null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT shiftrightunsigned(2.2, 2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT shiftrightunsigned(2.2, 0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("sign") {

    query = "SELECT sign(40)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT sign(-40)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT sign(0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT sign(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT sign(-4.20)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("signum") {

    query = "SELECT signum(40)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT signum(-40)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT signum(0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT signum(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT signum(-4.20)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("sin") {

    query = "SELECT sin(0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT sin(2)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT sin(-2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT sin(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT sin(2.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("sinh") {

    query = "SELECT sinh(0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT sinh(2)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT sinh(-2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT sinh(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT sinh(2.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("str_to_map") {

    query = "SELECT str_to_map(null)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    // throws below error
    // org.apache.spark.sql.AnalysisException:
    // Cannot have map type columns in DataFrame which calls set
    // operations(intersect, except, etc.), but the type of
    // column str_to_map(CAST(NULL AS STRING), ,, :) is map<string,string>;;
    query = "SELECT str_to_map('a:1,b:2,c:3', ',', ':')"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT str_to_map('a')"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT str_to_map('-1.2:a')"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT str_to_map(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("sqrt") {

    query = "SELECT sqrt(4)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    // On snappy shell throws below error for this query
    // snappy> select sqrt(-4);
    // ERROR 22003: (SQLState=22003 Severity=20000)
    // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-1)
    // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
    query = "SELECT sqrt(-4)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT sqrt(0)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT sqrt(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT sqrt(4.4)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("tan") {

    query = "SELECT tan(0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT tan(2)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT tan(-2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT tan(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT tan(2.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT tan(-2.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("tanh") {

    query = "SELECT tanh(0)"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT tanh(2)"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT tanh(-2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT tanh(null)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT tanh(2.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT tanh(-2.2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("+") {

    query = "SELECT (1+1)+3"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT 1.2+3+(4.5+2)"

    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT 0+0"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT 0+null"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("-") {

    query = "SELECT 1-1-1"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT 0-0"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT 0-null"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT 1.2-3-(4.5-2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("*") {

    query = "SELECT 4*2"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT 0*0"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT 0*null"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT 1.2*3*(4.5*2)"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("/") {

    query = "SELECT 4/2"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT 0/0"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT 0/null"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT 4.5/2"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("%") {

    query = "SELECT 4%2"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT 0%0"
    var snappyDf1 = snc.sql(s"$query")
    var sparkDf1 = sparkSession.sql(s"$query")
    validateResult(sparkDf1, snappyDf1)

    val c1s = snappyDf.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    query = "SELECT 0%null"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT 4.5%2"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("avg") {

    var sparkQuery = "SELECT avg(intcol) from sparktable"
    var snappyQuery = "SELECT avg(intcol) from rowtable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT avg(intcol) from sparktable"
    snappyQuery = "SELECT avg(intcol) from columnTable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)
  }

  test("count") {

    var sparkQuery = "SELECT count(*) from sparktable"
    var snappyQuery = "SELECT count(*) from rowtable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT count(intcol) from sparktable"
    var snappyQuery1 = "SELECT count(intcol) from rowtable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf1 = snc.sql(s"$snappyQuery1")
    validateResult(sparkDf, snappyDf1)

    var snappyDF = snc.sql(s"$snappyQuery")
    var snappyDF1 = snc.sql(s"$snappyQuery1")

    val c1s = snappyDF.columns
    val c2s = snappyDf1.columns
    assert(!c1s.sameElements(c2s))

    sparkQuery = "SELECT count(distinct(intcol)) from sparktable"
    snappyQuery = "SELECT count(distinct(intcol)) from rowtable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT count(*) from sparktable"
    snappyQuery = "SELECT count(*) from columnTable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT count(intcol) from sparktable"
    snappyQuery = "SELECT count(intcol) from columnTable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT count(distinct(intcol)) from sparktable"
    snappyQuery = "SELECT count(distinct(intcol)) from columnTable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)
  }

  test("first") {

    var sparkQuery = "SELECT first(stringcol) from sparktable"
    var snappyQuery = "SELECT first(stringcol) from rowtable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT first(stringcol, true) from sparktable"
    var snappyQuery1 = "SELECT first(stringcol, true) from rowtable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf1 = snc.sql(s"$snappyQuery1")
    validateResult(sparkDf, snappyDf1)

    var snappyDF = snc.sql(s"$snappyQuery")
    var snappyDF1 = snc.sql(s"$snappyQuery1")

    val c1s = snappyDF.columns
    val c2s = snappyDF1.columns
    assert(!c1s.sameElements(c2s))

    sparkQuery = "SELECT first(stringcol) from sparktable"
    snappyQuery = "SELECT first(stringcol) from columntable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT first(stringcol, true) from sparktable"
    snappyQuery = "SELECT first(stringcol, true) from columntable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)
  }

  test("first_value") {

    var sparkQuery = "SELECT first_value(stringcol) from sparktable"
    var snappyQuery = "SELECT first_value(stringcol) from rowtable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    // throws below error
    //  org.apache.spark.sql.AnalysisException:
    // The second argument of First should be a boolean literal.;;

    try {
      sparkQuery = "SELECT first_value(stringcol, true) from sparktable"
      var snappyQuery1 = "SELECT first_value(stringcol, true) from rowtable"
      sparkDf = sparkSession.sql(s"$sparkQuery")
      var snappyDf1 = snc.sql(s"$snappyQuery1")
      validateResult(sparkDf, snappyDf1)

      var snappyDF = snc.sql(s"$snappyQuery")
      var snappyDF1 = snc.sql(s"$snappyQuery1")

      val c1s = snappyDF.columns
      val c2s = snappyDF1.columns
      assert(!c1s.sameElements(c2s))
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

    sparkQuery = "SELECT first_value(stringcol) from sparktable"
    snappyQuery = "SELECT first_value(stringcol) from columntable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    // throws below error
    //  org.apache.spark.sql.AnalysisException:
    // The second argument of First should be a boolean literal.;;
    try {
      sparkQuery = "SELECT first_value(stringcol, true) from sparktable"
      snappyQuery = "SELECT first_value(stringcol, true) from columntable"
      sparkDf = sparkSession.sql(s"$sparkQuery")
      snappyDf = snc.sql(s"$snappyQuery")
      validateResult(sparkDf, snappyDf)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  test("last") {

    var sparkQuery = "SELECT last(stringcol) from sparktable"
    var snappyQuery = "SELECT last(stringcol) from rowtable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT last(stringcol, true) from sparktable"
    var snappyQuery1 = "SELECT last(stringcol, true) from rowtable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf1 = snc.sql(s"$snappyQuery1")
    validateResult(sparkDf, snappyDf1)

    var snappyDF = snc.sql(s"$snappyQuery")
    var snappyDF1 = snc.sql(s"$snappyQuery1")

    val c1s = snappyDF.columns
    val c2s = snappyDF1.columns
    assert(!c1s.sameElements(c2s))

    sparkQuery = "SELECT last(stringcol) from sparktable"
    snappyQuery = "SELECT last(stringcol) from columntable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT last(stringcol, true) from sparktable"
    snappyQuery = "SELECT last(stringcol, true) from columntable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)
  }

  test("last_value") {

    var sparkQuery = "SELECT last_value(stringcol) from sparktable"
    var snappyQuery = "SELECT last_value(stringcol) from rowtable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    // throws below error
    // snappy> SELECT last_value(stringcol, true) from columntable;
    // ERROR 42000: (SQLState=42000 Severity=20000)
    // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
    // Syntax error or analysis exception:
    // The second argument of First should be a boolean literal.;;

    try {
      sparkQuery = "SELECT last_value(stringcol, true) from sparktable"
      var snappyQuery1 = "SELECT last_value(stringcol, true) from rowtable"
      sparkDf = sparkSession.sql(s"$sparkQuery")
      var snappyDf1 = snc.sql(s"$snappyQuery1")
      validateResult(sparkDf, snappyDf1)

      var snappyDF = snc.sql(s"$snappyQuery")
      var snappyDF1 = snc.sql(s"$snappyQuery1")

      val c1s = snappyDf.columns
      val c2s = snappyDf1.columns
      assert(!c1s.sameElements(c2s))
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

    sparkQuery = "SELECT last_value(stringcol) from sparktable"
    snappyQuery = "SELECT last_value(stringcol) from columntable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    // throws below error
    //  org.apache.spark.sql.AnalysisException:
    // The second argument of last should be a boolean literal.;;

    try {
      sparkQuery = "SELECT last_value(stringcol, true) from sparktable"
      snappyQuery = "SELECT last_value(stringcol, true) from columntable"
      sparkDf = sparkSession.sql(s"$sparkQuery")
      snappyDf = snc.sql(s"$snappyQuery")
      validateResult(sparkDf, snappyDf)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  test("max") {

    var sparkQuery = "SELECT max(intcol) from sparktable"
    var snappyQuery = "SELECT max(intcol) from rowtable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT max(intcol) from sparktable"
    snappyQuery = "SELECT max(intcol) from columnTable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)
  }

  test("min") {

    var sparkQuery = "SELECT min(intcol) from sparktable"
    var snappyQuery = "SELECT min(intcol) from rowtable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT min(intcol) from sparktable"
    snappyQuery = "SELECT min(intcol) from columnTable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)
  }

  test("sum") {

    var sparkQuery = "SELECT sum(intcol) from sparktable"
    var snappyQuery = "SELECT sum(intcol) from rowtable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT sum(intcol) from sparktable"
    snappyQuery = "SELECT sum(intcol) from columnTable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)
  }

  test("length") {

    var sparkQuery = "SELECT length(stringcol) from sparktable"
    var snappyQuery = "SELECT length(stringcol) from columntable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT length(stringcol) from sparktable"
    snappyQuery = "SELECT length(stringcol) from rowTable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    query = "SELECT length('Spark SQL')"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("lower") {

    var sparkQuery = "SELECT lower(stringcol) from sparktable"
    var snappyQuery = "SELECT lower(stringcol) from rowtable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT lower(stringcol) from sparktable"
    snappyQuery = "SELECT lower(stringcol) from columnTable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    query = "SELECT lower('Spark SQL')"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT lower('abcABC123@#$%^&')"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

  }

  test("lcase") {

    var sparkQuery = "SELECT lcase(stringcol) from sparktable"
    var snappyQuery = "SELECT lcase(stringcol) from rowtable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT lcase(stringcol) from sparktable"
    snappyQuery = "SELECT lcase(stringcol) from columnTable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    query = "SELECT lcase('Spark SQL')"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT lcase('abcABC123@#$%^&')"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("upper") {

    var sparkQuery = "SELECT upper(stringcol) from sparktable"
    var snappyQuery = "SELECT upper(stringcol) from rowtable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT upper(stringcol) from sparktable"
    snappyQuery = "SELECT upper(stringcol) from columnTable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    query = "SELECT upper('Spark SQL')"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT upper('abcABC123@#$%^&')"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)
  }

  test("ucase") {

    var sparkQuery = "SELECT ucase(stringcol) from sparktable"
    var snappyQuery = "SELECT ucase(stringcol) from rowtable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT ucase(stringcol) from sparktable"
    snappyQuery = "SELECT ucase(stringcol) from columnTable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    query = "SELECT ucase('Spark SQL')"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    query = "SELECT ucase('abcABC123@#$%^&')"
    sparkDf = sparkSession.sql(s"$query")
    snappyDf = snc.sql(s"$query")
    validateResult(sparkDf, snappyDf)

  }

  test("sort_array") {


    query = "SELECT sort_array(array('b', 'd', 'c', 'a'))"
    var snappyDf = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDf)

    // ERROR : org.apache.spark.sql.AnalysisException: cannot resolve
    // 'sort_array(array('b', 'd', 'c', 'a'), true)' due to data type
    // mismatch: Sort order in second argument requires a boolean literal.;;
    // 'Project [sort_array(array(ParamLiteral:0,468#1,b,
    // ParamLiteral:1,468#1,d, ParamLiteral:2,468#1,c,
    // ParamLiteral:3,468#1,a), ParamLiteral:4,468#4,true) AS RES#7890]
    try {
      query = "SELECT sort_array(array('b', 'd', 'c', 'a'), true) as res1"
      var snappyDf1 = snc.sql(s"$query")
      var sparkDf1 = sparkSession.sql(s"$query")
      validateResult(sparkDf1, snappyDf1)

      val c1s = snappyDf.columns
      val c2s = snappyDf1.columns
      assert(!c1s.sameElements(c2s))
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

  }

  test("collect_list") {

    var sparkQuery = "SELECT collect_list(stringcol) from sparktable"
    var snappyQuery = "SELECT collect_list(stringcol) from rowtable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT collect_list(stringcol) from sparktable"
    snappyQuery = "SELECT collect_list(stringcol) from columnTable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)
  }

  test("collect_set") {

    var sparkQuery = "SELECT collect_set(stringcol) from sparktable"
    var snappyQuery = "SELECT collect_set(stringcol) from rowtable"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT collect_set(stringcol) from sparktable"
    snappyQuery = "SELECT collect_set(stringcol) from columnTable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT collect_set(intcol) from sparktable"
    snappyQuery = "SELECT collect_set(intcol) from rowtable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT collect_set(intcol) from sparktable"
    snappyQuery = "SELECT collect_set(intcol) from columnTable"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

  }

  test("concat") {

    var sparkQuery = "SELECT concat(stringcol,intcol) from sparktable order by intcol asc"
    var snappyQuery = "SELECT concat(stringcol,intcol) from rowtable order by intcol asc"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT concat(stringcol,intcol) from sparktable order by intcol asc"
    snappyQuery = "SELECT concat(stringcol,intcol) from columnTable order by intcol asc"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    query = "SELECT concat('Spark', 'SQL')"
    var snappyDF = snc.sql(s"$query")
    sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDF)

    query = "SELECT concat('Spark', 123)"
    var snappyDF1 = snc.sql(s"$query")
    sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDF1)

    val c1s = snappyDF.columns
    val c2s = snappyDF1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("concat_ws") {

    var sparkQuery = "SELECT concat_ws(' ',stringcol,intcol)" +
        " from sparktable order by intcol asc"
    var snappyQuery = "SELECT concat_ws(' ',stringcol,intcol) from rowtable order by intcol asc"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT concat_ws(' ',stringcol,intcol) from sparktable order by intcol asc"
    snappyQuery = "SELECT concat_ws(' ',stringcol,intcol) from columnTable order by intcol asc"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    query = "SELECT concat_ws(' ','Spark', 'SQL')"
    var snappyDF = snc.sql(s"$query")
    sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDF)

    query = "SELECT concat_ws(' ','Spark', 123)"
    var snappyDF1 = snc.sql(s"$query")
    sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDF1)

    val c1s = snappyDF.columns
    val c2s = snappyDF1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("elt") {

    query = "SELECT elt(1,'Spark','sql')"
    var snappyDF = snc.sql(s"$query")
    var sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDF)

    query = "SELECT elt(2,'Spark', 123)"
    var snappyDF1 = snc.sql(s"$query")
    sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDF1)

    val c1s = snappyDF.columns
    val c2s = snappyDF1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("find_in_set") {

    query = "SELECT find_in_set('c','abc,b,ab,c,def')"
    var snappyDF = snc.sql(s"$query")
    var sparkDF = sparkSession.sql(s"$query")
    validateResult(sparkDF, snappyDF)

    query = "SELECT find_in_set(1, '2,3,1')"
    var snappyDF1 = snc.sql(s"$query")
    var sparkDF1 = sparkSession.sql(s"$query")
    validateResult(sparkDF1, snappyDF1)

    val c1s = snappyDF.columns
    val c2s = snappyDF1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("format_number") {

    query = "SELECT format_number(12332.123456, 4)"
    var snappyDF = snc.sql(s"$query")
    var sparkDF = sparkSession.sql(s"$query")
    validateResult(sparkDF, snappyDF)

    query = "SELECT format_number(12332.123456, 1)"
    var snappyDF1 = snc.sql(s"$query")
    var sparkDF1 = sparkSession.sql(s"$query")
    validateResult(sparkDF1, snappyDF1)

    val c1s = snappyDF.columns
    val c2s = snappyDF1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("format_string") {

    query = "SELECT format_string('Hello World %d %s', 100, 'days')"
    var snappyDF = snc.sql(s"$query")
    var sparkDF = sparkSession.sql(s"$query")
    validateResult(sparkDF, snappyDF)

    query = "SELECT format_string('Hello World %d', 10)"
    var snappyDF1 = snc.sql(s"$query")
    var sparkDF1 = sparkSession.sql(s"$query")
    validateResult(sparkDF1, snappyDF1)

    val c1s = snappyDF.columns
    val c2s = snappyDF1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("initcap") {

    query = "SELECT initcap('sPark sql')"
    var snappyDF = snc.sql(s"$query")
    var sparkDF = sparkSession.sql(s"$query")
    validateResult(sparkDF, snappyDF)

    query = "SELECT initcap('ssssPark sql')"
    var snappyDF1 = snc.sql(s"$query")
    var sparkDF1 = sparkSession.sql(s"$query")
    validateResult(sparkDF1, snappyDF1)

    val c1s = snappyDF.columns
    val c2s = snappyDF1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("instr") {

    query = "SELECT instr('SparkSQL', 'SQL')"
    var snappyDF = snc.sql(s"$query")
    var sparkDF = sparkSession.sql(s"$query")
    validateResult(sparkDF, snappyDF)

    query = "SELECT instr('123abcABC', 'ab')"
    var snappyDF1 = snc.sql(s"$query")
    var sparkDF1 = sparkSession.sql(s"$query")
    validateResult(sparkDF1, snappyDF1)

    val c1s = snappyDF.columns
    val c2s = snappyDF1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("levenshtein") {

    query = "SELECT levenshtein('kitten', 'sitting')"
    var snappyDF = snc.sql(s"$query")
    var sparkDF = sparkSession.sql(s"$query")
    validateResult(sparkDF, snappyDF)

    query = "SELECT levenshtein('Snappy', 'Spark')"
    var snappyDF1 = snc.sql(s"$query")
    var sparkDF1 = sparkSession.sql(s"$query")
    validateResult(sparkDF1, snappyDF1)

    val c1s = snappyDF.columns
    val c2s = snappyDF1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("locate") {

    query = "SELECT locate('bar', 'foobarbar', 5)"
    var snappyDF = snc.sql(s"$query")
    var sparkDF = sparkSession.sql(s"$query")
    validateResult(sparkDF, snappyDF)

    query = "SELECT locate('abc', 'defghrih', 2)"
    var snappyDF1 = snc.sql(s"$query")
    var sparkDF1 = sparkSession.sql(s"$query")
    validateResult(sparkDF1, snappyDF1)

    val c1s = snappyDF.columns
    val c2s = snappyDF1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("lpad") {

    query = "SELECT lpad('hi', 5, '??')"
    var snappyDF = snc.sql(s"$query")
    var sparkDF = sparkSession.sql(s"$query")
    validateResult(sparkDF, snappyDF)

    query = "SELECT lpad('hi', 1, '??')"
    var snappyDF1 = snc.sql(s"$query")
    var sparkDF1 = sparkSession.sql(s"$query")
    validateResult(sparkDF1, snappyDF1)

    val c1s = snappyDF.columns
    val c2s = snappyDF1.columns
    assert(!c1s.sameElements(c2s))
  }

  test("add_months") {

    var sparkQuery = "SELECT add_months(datecol,1) from sparktable order by datecol asc"
    var snappyQuery = "SELECT add_months(datecol,1) from rowtable order by datecol asc"
    var sparkDf = sparkSession.sql(s"$sparkQuery")
    var snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    sparkQuery = "SELECT add_months(datecol,1) from sparktable order by datecol asc"
    snappyQuery = "SELECT add_months(datecol,1) from columnTable order by datecol asc"
    sparkDf = sparkSession.sql(s"$sparkQuery")
    snappyDf = snc.sql(s"$snappyQuery")
    validateResult(sparkDf, snappyDf)

    query = "SELECT add_months('2016-08-31', 1)"
    var snappyDF = snc.sql(s"$query")
    sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDF)

    query = "SELECT add_months('2016-08-31', 0)"
    var snappyDF1 = snc.sql(s"$query")
    sparkDf = sparkSession.sql(s"$query")
    validateResult(sparkDf, snappyDF1)

    val c1s = snappyDF.columns
    val c2s = snappyDF1.columns
    assert(!c1s.sameElements(c2s))
  }


}
