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

class SQLFunctionsTestSuite extends SnappyFunSuite
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll {

    // scalastyle:off println

    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    snc.sql("set snappydata.sql.tokenize=false")
    snc.sql("set snappydata.sql.planCaching=false")

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
        someDF.printSchema()
        someDF.createTempView("sparkTable")

        val sqlDF = sparkSession.sql("SELECT * FROM sparkTable")
        sqlDF.show()
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

    def assertQueryFullResultSet(snc: SnappyContext, sparkQuery: String,
        snappyQuery: String, numRows: Int,
        queryNum: String, tableType: String,
        pw: PrintWriter, sqlContext: SparkSession): Any = {

        var snappyDF = snc.sql(snappyQuery)
        val snappyQueryFileName = s"Snappy_$queryNum.out"
        val sparkQueryFileName = s"Spark_$queryNum.out"
        val snappyDest = getTempDir("snappyQueryFiles_" + tableType, onlyOnce = false)
        val sparkDest = getTempDir("sparkQueryFiles", onlyOnce = true)
        val sparkFile = new File(sparkDest, sparkQueryFileName)
        val snappyFile = new File(snappyDest, snappyQueryFileName)
        val col1 = snappyDF.schema.fieldNames(0)
        val col = snappyDF.schema.fieldNames.tail
        // snappyDF = snappyDF.sort(col1, col: _*)
        writeToFile(snappyDF, snappyFile, snc)
        // scalastyle:off println
        pw.println(s"$queryNum Result Collected in files with prefix $snappyFile")
        if (!new File(s"$sparkFile").exists()) {
            var sparkDF = sqlContext.sql(sparkQuery)
            val col = sparkDF.schema.fieldNames(0)
            val cols = sparkDF.schema.fieldNames.tail
            // sparkDF = sparkDF.sort(col, cols: _*)
            writeToFile(sparkDF, sparkFile, snc)
            pw.println(s"$queryNum Result Collected in files with prefix $sparkFile")
        }
        val expectedFiles = getSortedFiles(sparkFile).toIterator
        val actualFiles = getSortedFiles(snappyFile).toIterator
        val expectedLineSet = expectedFiles.flatMap(Source.fromFile(_).getLines())
        val actualLineSet = actualFiles.flatMap(Source.fromFile(_).getLines())
        var numLines = 0
        while (expectedLineSet.hasNext && actualLineSet.hasNext) {
            val expectedLine = expectedLineSet.next()
            val actualLine = actualLineSet.next()
            if (!actualLine.equals(expectedLine)) {
                pw.println(s"\n** For $queryNum result mismatch observed**")
                pw.println(s"\nExpected Result \n: $expectedLine")
                pw.println(s"\nActual Result   \n: $actualLine")
                pw.println(s"\nSnappy Query =" + snappyQuery + " Table Type : " + tableType)
                pw.println(s"\nSpark Query =" + sparkQuery + " Table Type : " + tableType)
                assert(false, s"\n** For $queryNum result mismatch observed** \n" +
                    s"Expected Result \n: $expectedLine \n" +
                    s"Actual Result   \n: $actualLine \n" +
                    s"Query =" + snappyQuery + " Table Type : " + tableType)
            }
            numLines += 1
        }
        if (actualLineSet.hasNext || expectedLineSet.hasNext) {
            pw.println(s"\nFor $queryNum result count mismatch observed")
            assert(false, s"\nFor $queryNum result count mismatch observed")
        }
        assert(numLines == numRows, s"\nFor $queryNum result count mismatch " +
            s"observed: Expected=$numRows, Got=$numLines")
        pw.flush()
        val snFile: String = snappyFile.toString + ".0"
        val spFile: String = sparkFile.toString + ".0"
        println("Query executed successfully" + snFile + " " + spFile)
        Files.delete(Paths.get(snFile))
        println(snappyFile.toString + " file deleted")
        Files.delete(Paths.get(spFile))
        println(sparkFile.toString + " file deleted")

    }

    test("abs") {

        query = "select abs(-1)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "abs_q1", "", pw, sparkSession)

        query = "select abs(1)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "abs_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "select abs(1.1)"
        assertQueryFullResultSet(snc, query, query, 1,
            "abs_q3", "", pw, sparkSession)

        query = "select abs(-1.1)"
        assertQueryFullResultSet(snc, query, query, 1,
            "abs_q4", "", pw, sparkSession)

        query = "select abs(0.0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "abs_q5", "", pw, sparkSession)

    }

    test("coalesce") {

        query = "SELECT COALESCE(NULL,NULL,NULL,'abc',NULL,'Example.com')"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "coalesce_q1", "", pw, sparkSession)

        query = "SELECT COALESCE(NULL, 1, 2, 'abc')"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "coalesce_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT COALESCE(1, 2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "coalesce_q3", "", pw, sparkSession)

        query = "SELECT COALESCE(NULL, NULL)"
        assertQueryFullResultSet(snc, query, query, 1,
            "coalesce_q4", "", pw, sparkSession)
    }

    test("cast") {

        // On snappy shell for below query throws error
        // snappy> select cast('NaN' as double);
        // ERROR 22003: (SQLState=22003 Severity=20000)
        // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
        // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
        query = "select cast('NaN' as double)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "cast_q1", "", pw, sparkSession)

        query = "SELECT CAST(25.65 AS varchar(12))"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "cast_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT cast('10' as int)"
        assertQueryFullResultSet(snc, query, query, 1,
            "cast_q3", "", pw, sparkSession)

        query = "SELECT CAST('2017-08-25' AS date)"
        assertQueryFullResultSet(snc, query, query, 1,
            "cast_q4", "", pw, sparkSession)

    }

    test("explode") {

        query = "SELECT explode(array(10, 20))"
        assertQueryFullResultSet(snc, query, query, 2,
            "cast_q1", "", pw, sparkSession)

        query = "SELECT explode(array(0))"
        assertQueryFullResultSet(snc, query, query, 1,
            "cast_q2", "", pw, sparkSession)

        query = "SELECT explode(array(NULL,1))"
        assertQueryFullResultSet(snc, query, query, 2,
            "cast_q3", "", pw, sparkSession)
    }

    test("greatest") {

        query = "SELECT greatest(10, 9, 2, 4, 3)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "greatest_q1", "", pw, sparkSession)

        query = "SELECT greatest(0, NULL)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "greatest_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("if") {

        query = "SELECT if(1 < 2, 'a', 'b')"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "if_q1", "", pw, sparkSession)

        query = "SELECT if(0 < NULL, 'a', 'b')"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "if_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("inline") {

        query = "SELECT inline(array(struct(1, 'a'), struct(2, 'b')))"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 2,
            "inline_q1", "", pw, sparkSession)

        query = "SELECT inline(array(struct(1), struct(2)))"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 2,
            "inline_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("isnan") {

        query = "SELECT isnan(cast('NaN' as double))"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "isnan_q1", "", pw, sparkSession)

        query = "SELECT isnan(123)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "isnan_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("ifnull") {

        query = "SELECT ifnull(NULL, array('2'))"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "ifnull_q1", "", pw, sparkSession)

        query = "SELECT ifnull(2, 3)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "ifnull_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("isnull") {

        query = "SELECT isnull(1)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "isnull_q1", "", pw, sparkSession)

        query = "SELECT isnull('abc')"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "isnull_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT isnull(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "isnull_q3", "", pw, sparkSession)
    }

    test("isnotnull") {

        query = "SELECT isnotnull(1)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "isnotnull_q1", "", pw, sparkSession)

        query = "SELECT isnotnull('abc')"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "isnotnull_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT isnotnull(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "isnotnull_q3", "", pw, sparkSession)
    }

    test("least") {

        query = "SELECT least(10, 9, 2, 4, 3)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "least_q1", "", pw, sparkSession)

        query = "SELECT least(null, 9, 3)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "least_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("nanvl") {

        query = "SELECT nanvl(cast('NaN' as double), 123)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "nanvl_q1", "", pw, sparkSession)

        // On snappy shell throws error for below query
        // snappy> SELECT nanvl(cast('NaN' as double), cast('NaN' as double));
        // ERROR 22003: (SQLState=22003 Severity=20000)
        // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
        // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
        query = "SELECT nanvl(cast('NaN' as double), cast('NaN' as double))"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "nanvl_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        // snappy> SELECT nanvl('NaN','NaN');
        // ERROR 22003: (SQLState=22003 Severity=20000)
        // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
        // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
        query = "SELECT nanvl('NaN','NaN')"
        assertQueryFullResultSet(snc, query, query, 1,
            "nanvl_q3", "", pw, sparkSession)

    }

    test("nullif") {

        query = "SELECT nullif(2, 2)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "nullif_q1", "", pw, sparkSession)

        query = "SELECT nullif( 9, 3)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "nullif_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT nullif( 9, 9, 4)"
        assertQueryFullResultSet(snc, query, query, 1,
            "nullif_q3", "", pw, sparkSession)

        query = "SELECT nullif( 9, 9, 9)"
        assertQueryFullResultSet(snc, query, query, 1,
            "nullif_q4", "", pw, sparkSession)

    }

    test("nvl") {
        query = "SELECT nvl(NULL, array('2'))"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "nvl_q1", "", pw, sparkSession)

        query = "SELECT nvl( 9, 3)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "nvl_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("nvl2") {

        query = "SELECT nvl2(NULL, 2, 1)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "nvl2_q1", "", pw, sparkSession)

        query = "SELECT nvl2( 9, 3, 1)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "nvl2_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("posexplode") {

        query = "SELECT posexplode(array(10,20))"
        assertQueryFullResultSet(snc, query, query, 2,
            "posexplode_q1", "", pw, sparkSession)

        query = "SELECT posexplode(array(10,0,null))"
        assertQueryFullResultSet(snc, query, query, 3,
            "posexplode_q2", "", pw, sparkSession)
    }

    test("rand") {
        query = "select rand()"
        var snappyDf = snc.sql(s"$query")
        snappyDf.show()

        query = "select rand(null)"
        var snappyDf1 = snc.sql(s"$query")
        snappyDf1.show()

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        // Throws error on snappy shell as well as in test
        // snappy> select rand(0);
        // ERROR 42000: (SQLState=42000 Severity=20000)
        // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
        // Syntax error or analysis exception: Input argument
        // to rand must be an integer, long or null literal.;
        query = "select rand(0)"
        snappyDf = snc.sql(s"$query")
        snappyDf.show()

        // Throws error on snappy shell as well as in test
        // snappy> select rand(2);
        // ERROR 42000: (SQLState=42000 Severity=20000)
        // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
        // Syntax error or analysis exception: Input argument
        // to rand must be an integer, long or null literal.;
        query = "select rand(2)"
        snappyDf = snc.sql(s"$query")
        snappyDf.show()


    }

    test("randn") {
        query = "select randn()"
        var snappyDf = snc.sql(s"$query")
        snappyDf.show()

        query = "select randn(null)"
        var snappyDf1 = snc.sql(s"$query")
        snappyDf1.show()

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        // Throws error on snappy shell as well as in test
        // snappy> select randn(0);
        // ERROR 42000: (SQLState=42000 Severity=20000)
        // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
        // Syntax error or analysis exception: Input argument
        // to randn must be an integer, long or null literal.;
        query = "select randn(0)"
        snappyDf = snc.sql(s"$query")
        snappyDf.show()

        // Throws error on snappy shell as well as in test
        // snappy> select randn(2);
        // ERROR 42000: (SQLState=42000 Severity=20000)
        // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
        // Syntax error or analysis exception: Input argument
        // to randn must be an integer, long or null literal.;
        query = "select randn(2)"
        snappyDf = snc.sql(s"$query")
        snappyDf.show()

    }

    test("stack") {

        query = "SELECT stack(2, 1, 2, 3)"
        assertQueryFullResultSet(snc, query, query, 2,
            "stack_q1", "", pw, sparkSession)

        query = "SELECT stack(2, 1, 2, 3, 4)"
        assertQueryFullResultSet(snc, query, query, 2,
            "stack_q2", "", pw, sparkSession)
    }

    test("when") {

        query = "SELECT case when 2>1 then 2 else 1 end"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "when_q1", "", pw, sparkSession)

        query = "SELECT case when 2<1 then 1 else 2 end"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "when_q2", "", pw, sparkSession)

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
        assertQueryFullResultSet(snc, query, query, 1,
            "acos_q1", "", pw, sparkSession)

        query = "SELECT acos(1)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "acos_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT acos(-1)"
        assertQueryFullResultSet(snc, query, query, 1,
            "acos_q3", "", pw, sparkSession)

        query = "SELECT acos(0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "acos_q4", "", pw, sparkSession)

        query = "SELECT acos(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "acos_q5", "", pw, sparkSession)

        query = "SELECT acos(2.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "acos_q6", "", pw, sparkSession)
    }

    test("asin") {

        query = "SELECT asin(0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "asin_q1", "", pw, sparkSession)

        // On snappy shell throws below error
        // snappy> SELECT asin(2);
        // ERROR 22003: (SQLState=22003 Severity=20000)
        // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
        // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
        query = "SELECT asin(2)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "asin_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT asin(-2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "asin_q3", "", pw, sparkSession)

        query = "SELECT asin(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "asin_q4", "", pw, sparkSession)

        query = "SELECT asin(2.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "asin_q5", "", pw, sparkSession)
    }

    test("atan") {

        query = "SELECT atan(0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "atan_q1", "", pw, sparkSession)

        query = "SELECT atan(2)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "atan_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT atan(-2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "atan_q3", "", pw, sparkSession)

        query = "SELECT atan(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "atan_q4", "", pw, sparkSession)

        query = "SELECT atan(2.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "atan_q5", "", pw, sparkSession)
    }

    test("atan2") {

        query = "SELECT atan2(0, 0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "atan2_q1", "", pw, sparkSession)

        query = "SELECT atan2(2, 3)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "atan2_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT atan2(2, null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "atan2_q3", "", pw, sparkSession)

        query = "SELECT atan2(2.2, 3)"
        assertQueryFullResultSet(snc, query, query, 1,
            "atan2_q4", "", pw, sparkSession)
    }

    test("bin") {

        query = "SELECT bin(13)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "bin_q1", "", pw, sparkSession)

        query = "SELECT bin(-13)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "bin_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT bin(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "bin_q3", "", pw, sparkSession)

        query = "SELECT bin(13.3)"
        assertQueryFullResultSet(snc, query, query, 1,
            "bin_q4", "", pw, sparkSession)
    }

    test("bround") {

        query = "SELECT bround(2.5, 0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "bround_q1", "", pw, sparkSession)

        query = "SELECT bround(2.5, 3)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "bround_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT bround(2.5, null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "bround_q3", "", pw, sparkSession)

        query = "SELECT round(0, null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "bround_q4", "", pw, sparkSession)
    }

    test("cbrt") {

        query = "SELECT cbrt(25)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "cbrt_q1", "", pw, sparkSession)

        query = "SELECT cbrt(0)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "cbrt_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT cbrt(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "cbrt_q3", "", pw, sparkSession)

        query = "SELECT cbrt(27.0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "cbrt_q4", "", pw, sparkSession)
    }

    test("ceil") {

        query = "SELECT ceil(-0.1)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "ceil_q1", "", pw, sparkSession)

        query = "SELECT ceil(5)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "ceil_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT ceil(0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "ceil_q3", "", pw, sparkSession)

        query = "SELECT ceil(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "ceil_q4", "", pw, sparkSession)
    }

    test("ceiling") {

        query = "SELECT ceiling(-0.1)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "ceiling_q1", "", pw, sparkSession)

        query = "SELECT ceiling(5)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "ceiling_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT ceiling(0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "ceiling_q3", "", pw, sparkSession)

        query = "SELECT ceiling(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "ceiling_q4", "", pw, sparkSession)
    }

    test("cos") {

        query = "SELECT cos(0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "cos_q1", "", pw, sparkSession)

        query = "SELECT cos(2)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "cos_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT cos(-2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "cos_q3", "", pw, sparkSession)

        query = "SELECT cos(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "cos_q4", "", pw, sparkSession)

        query = "SELECT cos(2.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "cos_q5", "", pw, sparkSession)
    }

    test("cosh") {

        query = "SELECT cosh(0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "cosh_q1", "", pw, sparkSession)

        query = "SELECT cosh(2)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "cosh_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT cosh(-2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "cosh_q3", "", pw, sparkSession)

        query = "SELECT cosh(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "cosh_q4", "", pw, sparkSession)

        query = "SELECT cosh(2.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "cosh_q5", "", pw, sparkSession)
    }

    test("conv") {

        query = "SELECT conv('100', 2, 10)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "conv_q1", "", pw, sparkSession)

        query = "SELECT conv(-10, 16, -10)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "conv_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("degrees") {

        query = "SELECT degrees(3.141592653589793)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "degrees_q1", "", pw, sparkSession)

        query = "SELECT degrees(6.283185307179586 )"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "degrees_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT degrees(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "degrees_q3", "", pw, sparkSession)

        query = "SELECT degrees(0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "degrees_q4", "", pw, sparkSession)
    }

    test("e") {

        query = "SELECT e()"
        assertQueryFullResultSet(snc, query, query, 1,
            "e_q1", "", pw, sparkSession)
    }

    test("exp") {

        query = "SELECT exp(0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "exp_q1", "", pw, sparkSession)

        query = "SELECT exp(2)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "exp_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT exp(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "exp_q3", "", pw, sparkSession)
    }

    test("expm1") {

        query = "SELECT expm1(0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "expm1_q1", "", pw, sparkSession)

        query = "SELECT expm1(2)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "expm1_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT expm1(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "expm1_q3", "", pw, sparkSession)
    }

    test("floor") {

        query = "SELECT floor(5)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "floor_q1", "", pw, sparkSession)

        query = "SELECT floor(null)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "floor_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT floor(0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "floor_q3", "", pw, sparkSession)

        query = "SELECT floor(-0.1)"
        assertQueryFullResultSet(snc, query, query, 1,
            "floor_q4", "", pw, sparkSession)
    }

    test("factorial") {

        query = "SELECT factorial(5)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "factorial_q1", "", pw, sparkSession)

        query = "SELECT factorial(-5)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "factorial_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT factorial(0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "factorial_q3", "", pw, sparkSession)

        query = "SELECT factorial(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "factorial_q4", "", pw, sparkSession)
    }

    test("hex") {

        query = "SELECT hex(17)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "hex_q1", "", pw, sparkSession)

        query = "SELECT hex(0)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "hex_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT hex(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "hex_q3", "", pw, sparkSession)

        query = "SELECT hex('Spark SQL')"
        assertQueryFullResultSet(snc, query, query, 1,
            "hex_q4", "", pw, sparkSession)


    }

    test("hypot") {

        query = "SELECT hypot(3, 4)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "hypot_q1", "", pw, sparkSession)

        query = "SELECT hypot(7,8)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "hypot_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT hypot(0,0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "hypot_q3", "", pw, sparkSession)

        query = "SELECT hypot(0,null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "hypot_q4", "", pw, sparkSession)

        query = "SELECT hypot(null,null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "hypot_q5", "", pw, sparkSession)
    }

    test("log") {

        query = "SELECT log(10, 100)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "log_q1", "", pw, sparkSession)

        query = "SELECT log(10,1000)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "log_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT log(10,0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "log_q3", "", pw, sparkSession)

        query = "SELECT log(10,null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "log_q4", "", pw, sparkSession)

        query = "SELECT log(10, 1000.234)"
        assertQueryFullResultSet(snc, query, query, 1,
            "log_q5", "", pw, sparkSession)
    }

    test("log10") {

        query = "SELECT log10(10)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "log10_q1", "", pw, sparkSession)

        query = "SELECT log10(0)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "log10_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT log10(-2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "log10_q3", "", pw, sparkSession)

        query = "SELECT log10(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "log10_q4", "", pw, sparkSession)

        query = "SELECT log10(1.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "log10_q5", "", pw, sparkSession)

    }

    test("log1p") {

        query = "SELECT log1p(0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "log1p_q1", "", pw, sparkSession)

        query = "SELECT log1p(2)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "log1p_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT log1p(-2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "log1p_q3", "", pw, sparkSession)

        query = "SELECT log1p(1.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "log1p_q4", "", pw, sparkSession)

        query = "SELECT log1p(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "log1p_q5", "", pw, sparkSession)
    }

    test("log2") {

        query = "SELECT log2(0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "log2_q1", "", pw, sparkSession)

        query = "SELECT log2(2)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "log2_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT log2(-2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "log2_q3", "", pw, sparkSession)

        query = "SELECT log2(1.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "log2_q4", "", pw, sparkSession)

        query = "SELECT log2(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "log2_q5", "", pw, sparkSession)
    }

    test("ln") {

        query = "SELECT ln(0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "ln_q1", "", pw, sparkSession)

        query = "SELECT ln(1)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "ln_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT ln(-1)"
        assertQueryFullResultSet(snc, query, query, 1,
            "ln_q3", "", pw, sparkSession)

        query = "SELECT ln(1.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "ln_q4", "", pw, sparkSession)

        query = "SELECT ln(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "ln_q5", "", pw, sparkSession)
    }

    test("negative") {

        query = "SELECT negative(0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "negative_q1", "", pw, sparkSession)

        query = "SELECT negative(1)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "negative_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT negative(-1)"
        assertQueryFullResultSet(snc, query, query, 1,
            "negative_q3", "", pw, sparkSession)

        query = "SELECT negative(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "negative_q4", "", pw, sparkSession)

        query = "SELECT negative(1.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "negative_q5", "", pw, sparkSession)

        query = "SELECT negative(-1.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "negative_q6", "", pw, sparkSession)

    }

    test("pi") {

        query = "SELECT pi()"
        assertQueryFullResultSet(snc, query, query, 1,
            "pi_q1", "", pw, sparkSession)
    }

    test("pmod") {

        query = "SELECT pmod(10,3)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "pmod_q1", "", pw, sparkSession)

        query = "SELECT pmod(-10,3)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "pmod_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT pmod(0,3)"
        assertQueryFullResultSet(snc, query, query, 1,
            "pmod_q3", "", pw, sparkSession)

        query = "SELECT pmod(null,3)"
        assertQueryFullResultSet(snc, query, query, 1,
            "pmod_q4", "", pw, sparkSession)

        query = "SELECT pmod(1.2,3)"
        assertQueryFullResultSet(snc, query, query, 1,
            "pmod_q5", "", pw, sparkSession)
    }

    test("positive") {

        query = "SELECT positive(0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "positive_q1", "", pw, sparkSession)

        query = "SELECT positive(1)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "positive_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT positive(-1)"
        assertQueryFullResultSet(snc, query, query, 1,
            "positive_q3", "", pw, sparkSession)

        query = "SELECT positive(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "positive_q4", "", pw, sparkSession)

        query = "SELECT positive(1.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "positive_q5", "", pw, sparkSession)

        query = "SELECT positive(-1.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "positive_q6", "", pw, sparkSession)

    }

    test("pow") {

        query = "SELECT pow(3,2)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "pow_q1", "", pw, sparkSession)

        query = "SELECT pow(-10,3)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "pow_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT pow(0,3)"
        assertQueryFullResultSet(snc, query, query, 1,
            "pow_q3", "", pw, sparkSession)

        query = "SELECT pow(null,3)"
        assertQueryFullResultSet(snc, query, query, 1,
            "pow_q4", "", pw, sparkSession)

        query = "SELECT pow(1.2,3)"
        assertQueryFullResultSet(snc, query, query, 1,
            "pow_q5", "", pw, sparkSession)
    }

    test("power") {

        query = "SELECT power(3,2)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "power_q1", "", pw, sparkSession)

        query = "SELECT power(-10,3)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "power_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT power(0,3)"
        assertQueryFullResultSet(snc, query, query, 1,
            "power_q3", "", pw, sparkSession)

        query = "SELECT power(null,3)"
        assertQueryFullResultSet(snc, query, query, 1,
            "power_q4", "", pw, sparkSession)

        query = "SELECT power(1.2,3)"
        assertQueryFullResultSet(snc, query, query, 1,
            "power_q5", "", pw, sparkSession)
    }

    test("radians") {

        query = "SELECT radians(360.0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "radians_q1", "", pw, sparkSession)

        query = "SELECT radians(180)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "radians_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT radians(0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "radians_q3", "", pw, sparkSession)

        query = "SELECT radians(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "radians_q4", "", pw, sparkSession)
    }

    test("rint") {

        query = "SELECT rint(12.3456)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "rint_q1", "", pw, sparkSession)

        query = "SELECT rint(-12.3456)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "rint_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT rint(180)"
        assertQueryFullResultSet(snc, query, query, 1,
            "rint_q3", "", pw, sparkSession)

        query = "SELECT rint(0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "rint_q4", "", pw, sparkSession)

        query = "SELECT rint(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "rint_q5", "", pw, sparkSession)
    }

    test("round") {

        query = "SELECT round(2.5, 0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "round_q1", "", pw, sparkSession)

        query = "SELECT round(2.5, 3)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "round_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT round(2.5, null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "round_q3", "", pw, sparkSession)

        query = "SELECT round(0, null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "round_q4", "", pw, sparkSession)
    }

    test("shiftleft") {

        query = "SELECT shiftleft(4, 1)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "shiftleft_q1", "", pw, sparkSession)

        query = "SELECT shiftleft(0, 1)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "shiftleft_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT shiftleft(null, null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "shiftleft_q3", "", pw, sparkSession)

        query = "SELECT shiftleft(2.2, 2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "shiftleft_q4", "", pw, sparkSession)

        query = "SELECT shiftleft(2.2, 0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "shiftleft_q5", "", pw, sparkSession)
    }

    test("shiftright") {

        query = "SELECT shiftright(4, 1)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "shiftright_q1", "", pw, sparkSession)

        query = "SELECT shiftright(0, 1)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "shiftright_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT shiftright(null, null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "shiftright_q3", "", pw, sparkSession)

        query = "SELECT shiftright(2.2, 2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "shiftright_q4", "", pw, sparkSession)

        query = "SELECT shiftright(2.2, 0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "shiftright_q5", "", pw, sparkSession)
    }

    test("shiftrightunsigned") {

        query = "SELECT shiftrightunsigned(4, 1)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "shiftrightunsigned_q1", "", pw, sparkSession)

        query = "SELECT shiftrightunsigned(0, 1)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "shiftrightunsigned_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT shiftrightunsigned(null, null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "shiftrightunsigned_q3", "", pw, sparkSession)

        query = "SELECT shiftrightunsigned(2.2, 2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "shiftrightunsigned_q4", "", pw, sparkSession)

        query = "SELECT shiftrightunsigned(2.2, 0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "shiftrightunsigned_q5", "", pw, sparkSession)
    }

    test("sign") {

        query = "SELECT sign(40)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "sign_q1", "", pw, sparkSession)

        query = "SELECT sign(-40)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "sign_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT sign(0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "sign_q3", "", pw, sparkSession)

        query = "SELECT sign(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "sign_q4", "", pw, sparkSession)

        query = "SELECT sign(-4.20)"
        assertQueryFullResultSet(snc, query, query, 1,
            "sign_q5", "", pw, sparkSession)
    }

    test("signum") {

        query = "SELECT signum(40)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "signum_q1", "", pw, sparkSession)

        query = "SELECT signum(-40)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "signum_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT signum(0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "signum_q3", "", pw, sparkSession)

        query = "SELECT signum(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "signum_q4", "", pw, sparkSession)

        query = "SELECT signum(-4.20)"
        assertQueryFullResultSet(snc, query, query, 1,
            "signum_q5", "", pw, sparkSession)
    }

    test("sin") {

        query = "SELECT sin(0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "sin_q1", "", pw, sparkSession)

        query = "SELECT sin(2)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "sin_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT sin(-2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "sin_q3", "", pw, sparkSession)

        query = "SELECT sin(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "sin_q4", "", pw, sparkSession)

        query = "SELECT sin(2.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "sin_q5", "", pw, sparkSession)
    }

    test("sinh") {

        query = "SELECT sinh(0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "sinh_q1", "", pw, sparkSession)

        query = "SELECT sinh(2)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "sinh_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT sinh(-2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "sinh_q3", "", pw, sparkSession)

        query = "SELECT sinh(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "sinh_q4", "", pw, sparkSession)

        query = "SELECT sinh(2.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "sinh_q5", "", pw, sparkSession)
    }

    test("str_to_map") {

        query = "SELECT str_to_map(null)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "str_to_map_q1", "", pw, sparkSession)

        // throws below error
        // org.apache.spark.sql.AnalysisException:
        // Cannot have map type columns in DataFrame which calls set
        // operations(intersect, except, etc.), but the type of
        // column str_to_map(CAST(NULL AS STRING), ,, :) is map<string,string>;;
        query = "SELECT str_to_map('a:1,b:2,c:3', ',', ':')"
        assertQueryFullResultSet(snc, query, query, 1,
            "str_to_map_q2", "", pw, sparkSession)

        query = "SELECT str_to_map('a')"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "str_to_map_q3", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT str_to_map('-1.2:a')"
        assertQueryFullResultSet(snc, query, query, 1,
            "str_to_map_q4", "", pw, sparkSession)

        query = "SELECT str_to_map(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "str_to_map_q5", "", pw, sparkSession)
    }

    test("sqrt") {

        query = "SELECT sqrt(4)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "sqrt_q1", "", pw, sparkSession)

        // On snappy shell throws below error for this query
        // snappy> select sqrt(-4);
        // ERROR 22003: (SQLState=22003 Severity=20000)
        // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-1)
        // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
        query = "SELECT sqrt(-4)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "sqrt_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT sqrt(0)"
        assertQueryFullResultSet(snc, query, query, 1,
            "sqrt_q3", "", pw, sparkSession)

        query = "SELECT sqrt(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "sqrt_q4", "", pw, sparkSession)

        query = "SELECT sqrt(4.4)"
        assertQueryFullResultSet(snc, query, query, 1,
            "sqrt_q5", "", pw, sparkSession)
    }

    test("tan") {

        query = "SELECT tan(0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "tan_q1", "", pw, sparkSession)

        query = "SELECT tan(2)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "tan_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT tan(-2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "tan_q3", "", pw, sparkSession)

        query = "SELECT tan(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "tan_q4", "", pw, sparkSession)

        query = "SELECT tan(2.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "tan_q5", "", pw, sparkSession)

        query = "SELECT tan(-2.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "tan_q6", "", pw, sparkSession)
    }

    test("tanh") {

        query = "SELECT tanh(0)"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "tanh_q1", "", pw, sparkSession)

        query = "SELECT tanh(2)"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "tanh_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT tanh(-2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "tanh_q3", "", pw, sparkSession)

        query = "SELECT tanh(null)"
        assertQueryFullResultSet(snc, query, query, 1,
            "tanh_q4", "", pw, sparkSession)

        query = "SELECT tanh(2.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "tanh_q5", "", pw, sparkSession)

        query = "SELECT tanh(-2.2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "tanh_q6", "", pw, sparkSession)
    }

    test("+") {

        query = "SELECT (1+1)+3"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "op+_q1", "", pw, sparkSession)

        query = "SELECT 1.2+3+(4.5+2)"

        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "op+_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT 0+0"
        assertQueryFullResultSet(snc, query, query, 1,
            "op+_q3", "", pw, sparkSession)

        query = "SELECT 0+null"
        assertQueryFullResultSet(snc, query, query, 1,
            "op+_q4", "", pw, sparkSession)
    }

    test("-") {

        query = "SELECT 1-1-1"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "op-_q1", "", pw, sparkSession)

        query = "SELECT 0-0"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "op-_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT 0-null"
        assertQueryFullResultSet(snc, query, query, 1,
            "op-_q3", "", pw, sparkSession)

        query = "SELECT 1.2-3-(4.5-2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "op-_q4", "", pw, sparkSession)
    }

    test("*") {

        query = "SELECT 4*2"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "op*_q1", "", pw, sparkSession)

        query = "SELECT 0*0"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "op*_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT 0*null"
        assertQueryFullResultSet(snc, query, query, 1,
            "op*_q3", "", pw, sparkSession)

        query = "SELECT 1.2*3*(4.5*2)"
        assertQueryFullResultSet(snc, query, query, 1,
            "op*_q4", "", pw, sparkSession)
    }

    test("/") {

        query = "SELECT 4/2"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "op/_q1", "", pw, sparkSession)

        query = "SELECT 0/0"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "op/_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT 0/null"
        assertQueryFullResultSet(snc, query, query, 1,
            "op/_q3", "", pw, sparkSession)

        query = "SELECT 4.5/2"
        assertQueryFullResultSet(snc, query, query, 1,
            "op/_q4", "", pw, sparkSession)
    }

    test("%") {

        query = "SELECT 4%2"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "op%_q1", "", pw, sparkSession)

        query = "SELECT 0%0"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "op%_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        query = "SELECT 0%null"
        assertQueryFullResultSet(snc, query, query, 1,
            "op%_q3", "", pw, sparkSession)

        query = "SELECT 4.5%2"
        assertQueryFullResultSet(snc, query, query, 1,
            "op%_q4", "", pw, sparkSession)
    }

    test("avg") {

        var sparkQuery = "SELECT avg(intcol) from sparktable"
        var snappyQuery = "SELECT avg(intcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "avg_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT avg(intcol) from sparktable"
        snappyQuery = "SELECT avg(intcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "avg_q2", "ColumnTable", pw, sparkSession)
    }

    test("count") {

        var sparkQuery = "SELECT count(*) from sparktable"
        var snappyQuery = "SELECT count(*) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "count_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT count(intcol) from sparktable"
        var snappyQuery1 = "SELECT count(intcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery1, 1,
            "count_q2", "RowTable", pw, sparkSession)

        var snappyDf = snc.sql(s"$snappyQuery")
        var snappyDf1 = snc.sql(s"$snappyQuery1")

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        sparkQuery = "SELECT count(distinct(intcol)) from sparktable"
        snappyQuery = "SELECT count(distinct(intcol)) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "count_q3", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT count(*) from sparktable"
        snappyQuery = "SELECT count(*) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "count_q4", "ColumnTable", pw, sparkSession)

        sparkQuery = "SELECT count(intcol) from sparktable"
        snappyQuery = "SELECT count(intcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "count_q5", "ColumnTable", pw, sparkSession)

        sparkQuery = "SELECT count(distinct(intcol)) from sparktable"
        snappyQuery = "SELECT count(distinct(intcol)) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "count_q6", "ColumnTable", pw, sparkSession)
    }

    test("first") {

        var sparkQuery = "SELECT first(stringcol) from sparktable"
        var snappyQuery = "SELECT first(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "first_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT first(stringcol, true) from sparktable"
        var snappyQuery1 = "SELECT first(stringcol, true) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery1, 1,
            "first_q2", "RowTable", pw, sparkSession)

        var snappyDf = snc.sql(s"$snappyQuery")
        var snappyDf1 = snc.sql(s"$snappyQuery1")

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        sparkQuery = "SELECT first(stringcol) from sparktable"
        snappyQuery = "SELECT first(stringcol) from columntable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "first_q3", "ColumnTable", pw, sparkSession)

        sparkQuery = "SELECT first(stringcol, true) from sparktable"
        snappyQuery = "SELECT first(stringcol, true) from columntable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "first_q4", "ColumnTable", pw, sparkSession)
    }

    test("first_value") {

        var sparkQuery = "SELECT first_value(stringcol) from sparktable"
        var snappyQuery = "SELECT first_value(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "first_value_q1", "RowTable", pw, sparkSession)

        // throws below error
        //  org.apache.spark.sql.AnalysisException:
        // The second argument of First should be a boolean literal.;;
        sparkQuery = "SELECT first_value(stringcol, true) from sparktable"
        var snappyQuery1 = "SELECT first_value(stringcol, true) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery1, 1,
            "first_value_q2", "RowTable", pw, sparkSession)

        var snappyDf = snc.sql(s"$snappyQuery")
        var snappyDf1 = snc.sql(s"$snappyQuery1")

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        sparkQuery = "SELECT first_value(stringcol) from sparktable"
        snappyQuery = "SELECT first_value(stringcol) from columntable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "first_value_q3", "ColumnTable", pw, sparkSession)

        // throws below error
        //  org.apache.spark.sql.AnalysisException:
        // The second argument of First should be a boolean literal.;;
        sparkQuery = "SELECT first_value(stringcol, true) from sparktable"
        snappyQuery = "SELECT first_value(stringcol, true) from columntable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "first_value_q4", "ColumnTable", pw, sparkSession)
    }

    test("last") {

        var sparkQuery = "SELECT last(stringcol) from sparktable"
        var snappyQuery = "SELECT last(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "last_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT last(stringcol, true) from sparktable"
        var snappyQuery1 = "SELECT last(stringcol, true) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery1, 1,
            "last_q2", "RowTable", pw, sparkSession)

        var snappyDf = snc.sql(s"$snappyQuery")
        var snappyDf1 = snc.sql(s"$snappyQuery1")

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        sparkQuery = "SELECT last(stringcol) from sparktable"
        snappyQuery = "SELECT last(stringcol) from columntable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "last_q3", "ColumnTable", pw, sparkSession)

        sparkQuery = "SELECT last(stringcol, true) from sparktable"
        snappyQuery = "SELECT last(stringcol, true) from columntable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "last_q4", "ColumnTable", pw, sparkSession)
    }

    test("last_value") {

        var sparkQuery = "SELECT last_value(stringcol) from sparktable"
        var snappyQuery = "SELECT last_value(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "last_value_q1", "RowTable", pw, sparkSession)

        // throws below error
        // snappy> SELECT last_value(stringcol, true) from columntable;
        // ERROR 42000: (SQLState=42000 Severity=20000)
        // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
        // Syntax error or analysis exception:
        // The second argument of First should be a boolean literal.;;
        sparkQuery = "SELECT last_value(stringcol, true) from sparktable"
        var snappyQuery1 = "SELECT last_value(stringcol, true) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery1, 1,
            "last_value_q2", "RowTable", pw, sparkSession)

        var snappyDf = snc.sql(s"$snappyQuery")
        var snappyDf1 = snc.sql(s"$snappyQuery1")

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))

        sparkQuery = "SELECT last_value(stringcol) from sparktable"
        snappyQuery = "SELECT last_value(stringcol) from columntable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "last_value_q3", "ColumnTable", pw, sparkSession)

        // throws below error
        //  org.apache.spark.sql.AnalysisException:
        // The second argument of last should be a boolean literal.;;
        sparkQuery = "SELECT last_value(stringcol, true) from sparktable"
        snappyQuery = "SELECT last_value(stringcol, true) from columntable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "last_value_q4", "ColumnTable", pw, sparkSession)
    }

    test("max") {

        var sparkQuery = "SELECT max(intcol) from sparktable"
        var snappyQuery = "SELECT max(intcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "max_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT max(intcol) from sparktable"
        snappyQuery = "SELECT max(intcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "max_q2", "ColumnTable", pw, sparkSession)
    }

    test("min") {

        var sparkQuery = "SELECT min(intcol) from sparktable"
        var snappyQuery = "SELECT min(intcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "min_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT min(intcol) from sparktable"
        snappyQuery = "SELECT min(intcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "min_q2", "ColumnTable", pw, sparkSession)
    }

    test("sum") {

        var sparkQuery = "SELECT sum(intcol) from sparktable"
        var snappyQuery = "SELECT sum(intcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "sum_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT sum(intcol) from sparktable"
        snappyQuery = "SELECT sum(intcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "sum_q2", "ColumnTable", pw, sparkSession)
    }

    test("length") {

        var sparkQuery = "SELECT length(stringcol) from sparktable"
        var snappyQuery = "SELECT length(stringcol) from columntable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "length_q1", "ColumnTable", pw, sparkSession)

        sparkQuery = "SELECT length(stringcol) from sparktable"
        snappyQuery = "SELECT length(stringcol) from rowTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "length_q2", "RowTable", pw, sparkSession)

        query = "SELECT length('Spark SQL')"
        assertQueryFullResultSet(snc, query, query, 1,
            "length_q3", "", pw, sparkSession)
    }

    test("lower") {

        var sparkQuery = "SELECT lower(stringcol) from sparktable"
        var snappyQuery = "SELECT lower(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "lower_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT lower(stringcol) from sparktable"
        snappyQuery = "SELECT lower(stringcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "lower_q2", "ColumnTable", pw, sparkSession)

        query = "SELECT lower('Spark SQL')"
        assertQueryFullResultSet(snc, query, query, 1,
            "lower_q3", "", pw, sparkSession)

        query = "SELECT lower('abcABC123@#$%^&')"
        assertQueryFullResultSet(snc, query, query, 1,
            "lower_q4", "", pw, sparkSession)

    }

    test("lcase") {

        var sparkQuery = "SELECT lcase(stringcol) from sparktable"
        var snappyQuery = "SELECT lcase(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "lcase_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT lcase(stringcol) from sparktable"
        snappyQuery = "SELECT lcase(stringcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "lcase_q2", "ColumnTable", pw, sparkSession)

        query = "SELECT lcase('Spark SQL')"
        assertQueryFullResultSet(snc, query, query, 1,
            "lcase_q3", "", pw, sparkSession)

        query = "SELECT lcase('abcABC123@#$%^&')"
        assertQueryFullResultSet(snc, query, query, 1,
            "lcase_q4", "", pw, sparkSession)
    }

    test("upper") {

        var sparkQuery = "SELECT upper(stringcol) from sparktable"
        var snappyQuery = "SELECT upper(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "upper_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT upper(stringcol) from sparktable"
        snappyQuery = "SELECT upper(stringcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "upper_q2", "ColumnTable", pw, sparkSession)

        query = "SELECT upper('Spark SQL')"
        assertQueryFullResultSet(snc, query, query, 1,
            "upper_q3", "", pw, sparkSession)

        query = "SELECT upper('abcABC123@#$%^&')"
        assertQueryFullResultSet(snc, query, query, 1,
            "upper_q4", "", pw, sparkSession)
    }

    test("ucase") {

        var sparkQuery = "SELECT ucase(stringcol) from sparktable"
        var snappyQuery = "SELECT ucase(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "ucase_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT ucase(stringcol) from sparktable"
        snappyQuery = "SELECT ucase(stringcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "ucase_q2", "ColumnTable", pw, sparkSession)

        query = "SELECT ucase('Spark SQL')"
        assertQueryFullResultSet(snc, query, query, 1,
            "ucase_q3", "", pw, sparkSession)

        query = "SELECT ucase('abcABC123@#$%^&')"
        assertQueryFullResultSet(snc, query, query, 1,
            "ucase_q4", "", pw, sparkSession)

    }

    test("sort_array") {


        query = "SELECT sort_array(array('b', 'd', 'c', 'a'))"
        var snappyDf = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "sort_array_q1", "", pw, sparkSession)

        // ERROR : org.apache.spark.sql.AnalysisException: cannot resolve
        // 'sort_array(array('b', 'd', 'c', 'a'), true)' due to data type
        // mismatch: Sort order in second argument requires a boolean literal.;;
        // 'Project [sort_array(array(ParamLiteral:0,468#1,b,
        // ParamLiteral:1,468#1,d, ParamLiteral:2,468#1,c,
        // ParamLiteral:3,468#1,a), ParamLiteral:4,468#4,true) AS RES#7890]
        query = "SELECT sort_array(array('b', 'd', 'c', 'a'), true) as res1"
        var snappyDf1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "sort_array_q2", "", pw, sparkSession)

        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("collect_list") {

        var sparkQuery = "SELECT collect_list(stringcol) from sparktable"
        var snappyQuery = "SELECT collect_list(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "collect_list_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT collect_list(stringcol) from sparktable"
        snappyQuery = "SELECT collect_list(stringcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "collect_list_q2", "ColumnTable", pw, sparkSession)
    }

    test("collect_set") {

        var sparkQuery = "SELECT collect_set(stringcol) from sparktable"
        var snappyQuery = "SELECT collect_set(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "collect_set_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT collect_set(stringcol) from sparktable"
        snappyQuery = "SELECT collect_set(stringcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "collect_set_q2", "ColumnTable", pw, sparkSession)

        sparkQuery = "SELECT collect_set(intcol) from sparktable"
        snappyQuery = "SELECT collect_set(intcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "collect_set_q3", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT collect_set(intcol) from sparktable"
        snappyQuery = "SELECT collect_set(intcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "collect_set_q4", "ColumnTable", pw, sparkSession)

    }

    test("concat") {

        var sparkQuery = "SELECT concat(stringcol,intcol) from sparktable order by intcol asc"
        var snappyQuery = "SELECT concat(stringcol,intcol) from rowtable order by intcol asc"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "concat_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT concat(stringcol,intcol) from sparktable order by intcol asc"
        snappyQuery = "SELECT concat(stringcol,intcol) from columnTable order by intcol asc"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "concat_q2", "ColumnTable", pw, sparkSession)

        query = "SELECT concat('Spark', 'SQL')"
        var snappyDF = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "concat_q3", "", pw, sparkSession)

        query = "SELECT concat('Spark', 123)"
        var snappyDF1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "concat_q4", "", pw, sparkSession)

        val c1s = snappyDF.columns
        val c2s = snappyDF1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("concat_ws") {

        var sparkQuery = "SELECT concat_ws(' ',stringcol,intcol)" +
            " from sparktable order by intcol asc"
        var snappyQuery = "SELECT concat_ws(' ',stringcol,intcol) from rowtable order by intcol asc"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "concat_ws_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT concat_ws(' ',stringcol,intcol) from sparktable order by intcol asc"
        snappyQuery = "SELECT concat_ws(' ',stringcol,intcol) from columnTable order by intcol asc"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "concat_ws_q2", "ColumnTable", pw, sparkSession)

        query = "SELECT concat_ws(' ','Spark', 'SQL')"
        var snappyDF = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "concat_ws_q3", "", pw, sparkSession)

        query = "SELECT concat_ws(' ','Spark', 123)"
        var snappyDF1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "concat_ws_q4", "", pw, sparkSession)

        val c1s = snappyDF.columns
        val c2s = snappyDF1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("elt") {

        query = "SELECT elt(1,'Spark','sql')"
        var snappyDF = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "elt_q1", "", pw, sparkSession)

        query = "SELECT elt(2,'Spark', 123)"
        var snappyDF1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "elt_q2", "", pw, sparkSession)

        val c1s = snappyDF.columns
        val c2s = snappyDF1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("find_in_set") {

        query = "SELECT find_in_set('c','abc,b,ab,c,def')"
        var snappyDF = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "find_in_set_q1", "", pw, sparkSession)

        query = "SELECT find_in_set(1, '2,3,1')"
        var snappyDF1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "find_in_set_q2", "", pw, sparkSession)

        val c1s = snappyDF.columns
        val c2s = snappyDF1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("format_number") {

        query = "SELECT format_number(12332.123456, 4)"
        var snappyDF = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "format_number_q1", "", pw, sparkSession)

        query = "SELECT format_number(12332.123456, 1)"
        var snappyDF1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "format_number_q2", "", pw, sparkSession)

        val c1s = snappyDF.columns
        val c2s = snappyDF1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("format_string") {

        query = "SELECT format_string('Hello World %d %s', 100, 'days')"
        var snappyDF = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "format_string_q1", "", pw, sparkSession)

        query = "SELECT format_string('Hello World %d', 10)"
        var snappyDF1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "format_string_q2", "", pw, sparkSession)

        val c1s = snappyDF.columns
        val c2s = snappyDF1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("initcap") {

        query = "SELECT initcap('sPark sql')"
        var snappyDF = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "initcap_q1", "", pw, sparkSession)

        query = "SELECT initcap('ssssPark sql')"
        var snappyDF1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "initcap_q2", "", pw, sparkSession)

        val c1s = snappyDF.columns
        val c2s = snappyDF1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("instr") {

        query = "SELECT instr('SparkSQL', 'SQL')"
        var snappyDF = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "instr_q1", "", pw, sparkSession)

        query = "SELECT instr('123abcABC', 'ab')"
        var snappyDF1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "instr_q2", "", pw, sparkSession)

        val c1s = snappyDF.columns
        val c2s = snappyDF1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("levenshtein") {

        query = "SELECT levenshtein('kitten', 'sitting')"
        var snappyDF = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "levenshtein_q1", "", pw, sparkSession)

        query = "SELECT levenshtein('Snappy', 'Spark')"
        var snappyDF1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "levenshtein_q2", "", pw, sparkSession)

        val c1s = snappyDF.columns
        val c2s = snappyDF1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("locate") {

        query = "SELECT locate('bar', 'foobarbar', 5)"
        var snappyDF = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "locate_q1", "", pw, sparkSession)

        query = "SELECT locate('abc', 'defghrih', 2)"
        var snappyDF1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "locate_q2", "", pw, sparkSession)

        val c1s = snappyDF.columns
        val c2s = snappyDF1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("lpad") {

        query = "SELECT lpad('hi', 5, '??')"
        var snappyDF = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "lpad_q1", "", pw, sparkSession)

        query = "SELECT lpad('hi', 1, '??')"
        var snappyDF1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "lpad_q2", "", pw, sparkSession)

        val c1s = snappyDF.columns
        val c2s = snappyDF1.columns
        assert(!c1s.sameElements(c2s))
    }

    test("add_months") {

        var sparkQuery = "SELECT add_months(datecol,1) from sparktable order by datecol asc"
        var snappyQuery = "SELECT add_months(datecol,1) from rowtable order by datecol asc"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "add_months_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT add_months(datecol,1) from sparktable order by datecol asc"
        snappyQuery = "SELECT add_months(datecol,1) from columnTable order by datecol asc"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 2,
            "add_months_q2", "ColumnTable", pw, sparkSession)

        query = "SELECT add_months('2016-08-31', 1)"
        var snappyDF = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "add_months_q3", "", pw, sparkSession)

        query = "SELECT add_months('2016-08-31', 0)"
        var snappyDF1 = snc.sql(s"$query")
        assertQueryFullResultSet(snc, query, query, 1,
            "add_months_q4", "", pw, sparkSession)

        val c1s = snappyDF.columns
        val c2s = snappyDF1.columns
        assert(!c1s.sameElements(c2s))
    }


}
