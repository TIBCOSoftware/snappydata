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
package io.snappydata.externalstore

import java.io.{File, FileOutputStream, PrintWriter}
import java.math.BigDecimal
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Calendar
import java.sql.{Date, Timestamp}

import scala.io.Source
import scala.language.postfixOps
import java.nio.file.Paths
import io.snappydata.SnappyFunSuite
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.sql.NorthWindDUnitTest.writeToFile
import org.apache.spark.sql.types._
import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, Row, SnappyContext, SparkSession}

class SQLFunctionsTestSuite extends SnappyFunSuite
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll{

    // scalastyle:off println

    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()

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
        // val str = "hello".getBytes()


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
            Row(1000, null, null, null , "1234567890abcdefghij",
                date1 , new BigDecimal(66), 2.2, 1.0E8f,
                1000, 1000, "1234567890abcdefghij", new BigDecimal(100000.0),
                new BigDecimal(100000.0), 2.2 , null, "abcd",
                time1, "abcd'", null, null, null, null ),
            Row(-10, null, true, null , "ABC@#",
                current_date , new BigDecimal(-66), 0.0111, -2.225E-307f,
                -10, 10, "ABC@#", new BigDecimal(-1),
                new BigDecimal(1), 123.56 , 0.089f, "abcd",
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

    def validateResult(sparkDf: DataFrame, snappyDf: DataFrame): Unit = {

        val sparkColumns = sparkDf.schema.fields.map(_.name)

        val selectiveDifferences = sparkColumns.map(col =>
            sparkDf.select(col).except(snappyDf.select(col)))
        sparkDf.show()
        snappyDf.show()
        println("selective difference " + selectiveDifferences)
        selectiveDifferences.map(diff => {
            println(diff)
            if (diff.count > 0) {
                diff.show()
                println(s"For query '$query' result mismatched observed")
            }
            else println(s"For query '$query' result matched observed")
        })


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
        snappyDF = snappyDF.sort(col1, col: _*)
        writeToFile(snappyDF, snappyFile, snc)
        // scalastyle:off println
        pw.println(s"$queryNum Result Collected in files with prefix $snappyFile")
        if (!new File(s"$sparkFile").exists()) {
            var sparkDF = sqlContext.sql(sparkQuery)
            val col = sparkDF.schema.fieldNames(0)
            val cols = sparkDF.schema.fieldNames.tail
            sparkDF = sparkDF.sort(col, cols: _*)
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
        /* val snFile: String = snappyFile.toString
        val spFile: String = sparkFile.toString

        val snPathToFile = Paths.get(snFile)
        val spPathToFile = Paths.get(spFile)
        println("Query executed successfully" + snFile + " " + spFile)
        Files.delete(snPathToFile)
        println(snappyFile.toString + " file deleted")
        Files.delete(spPathToFile)
        println(sparkFile.toString + " file deleted") */

    }

    test("abs"){

        query = "select abs(-1)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "select abs(1)"
        var sparkDf1 = sparkSession.sql(s"$query")
        var snappyDf1 = snc.sql(s"$query")
        val c1s = snappyDf.columns
        val c2s = snappyDf1.columns
        assert(!c1s.sameElements(c2s))
        validateResult(sparkDf1, snappyDf1)


        // without alias throws error
        query = "select abs(1.1) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "select abs(-1.1) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "select abs(0.0) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

    }

    test("coalesce"){
        // without alias throwing error
        query = "SELECT COALESCE(NULL,NULL,NULL,'abc',NULL,'Example.com') as res"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT COALESCE(NULL, 1, 2, 'abc')as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)


        query = "SELECT COALESCE(1, 2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT COALESCE(NULL, NULL) as res" // throws error
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

    }

    test("cast"){
        // without alias throws error for all query

        // On snappy shell for below query throws error
        // snappy> select cast('NaN' as double) as res;
        // ERROR 22003: (SQLState=22003 Severity=20000)
        // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
        // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
        query = "select cast('NaN' as double) as res"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT CAST(25.65 AS varchar(12)) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT cast('10' as int) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT CAST('2017-08-25' AS date) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

    }

    test("explode"){
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

    test("greatest"){
        query = "SELECT greatest(10, 9, 2, 4, 3)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT greatest(0, NULL)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("if"){
        query = "SELECT if(1 < 2, 'a', 'b')"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT if(0 < NULL, 'a', 'b')"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("inline"){
        query = "SELECT inline(array(struct(1, 'a'), struct(2, 'b')))"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT inline(array(struct(1), struct(2)))"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("isnan"){
        // without alias throws error for below query
        query = "SELECT isnan(cast('NaN' as double)) as res"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT isnan(123)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("ifnull"){
        query = "SELECT ifnull(NULL, array('2'))"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT ifnull(2, 3)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("isnull"){
        query = "SELECT isnull(1)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error for below query
        query = "SELECT isnull('abc') as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT isnull(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("isnotnull"){
        query = "SELECT isnotnull(1)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error for below query
        query = "SELECT isnotnull('abc') as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT isnotnull(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("least"){
        query = "SELECT least(10, 9, 2, 4, 3)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT least(null, 9, 3)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("nanvl"){
        // without alias throws error for all below query
        query = "SELECT nanvl(cast('NaN' as double), 123) as res"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // On snappy shell throws error for below query
        // snappy> SELECT nanvl(cast('NaN' as double), cast('NaN' as double));
        // ERROR 22003: (SQLState=22003 Severity=20000)
        // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
        // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
        query = "SELECT nanvl(cast('NaN' as double), cast('NaN' as double)) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // snappy> SELECT nanvl('NaN','NaN');
        // ERROR 22003: (SQLState=22003 Severity=20000)
        // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
        // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
        query = "SELECT nanvl('NaN','NaN') as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

    }

    test("nullif"){
        query = "SELECT nullif(2, 2)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT nullif( 9, 3)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT nullif( 9, 9, 4)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT nullif( 9, 9, 9)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

    }

    test("nvl"){
        query = "SELECT nvl(NULL, array('2'))"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT nvl( 9, 3)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("posexplode"){
        query = "SELECT posexplode(array(10,20))"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT posexplode(array(10,0,null))"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("rand") {
        query = "select rand()"
        var snappyDf = snc.sql(s"$query")
        snappyDf.show()

        query = "select rand(null)"
        snappyDf = snc.sql(s"$query")
        snappyDf.show()

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
        snappyDf = snc.sql(s"$query")
        snappyDf.show()

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

    test("stack"){
        query = "SELECT stack(2, 1, 2, 3)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // Throws error on snappy shell as well as in test
        // snappy> SELECT stack(2, 1, 2, 3, 4);
        // ERROR 42X01: (SQLState=42X01 Severity=20000)
        // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
        // Syntax error: java.lang.AssertionError: assertion failed;.
        // Issue the 'help' command for general information on SnappyData command syntax.
        // Any unrecognized commands are treated as potential SQL commands and executed directly.
        // Consult your DBMS server reference documentation for
        // details of the SQL syntax supported by your server.
        query = "SELECT stack(2, 1, 2, 3, 4)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("when"){
        query = "SELECT case when 2>1 then 2 else 1 end"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT case when 2<1 then 1 else 2 end"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("acos"){
       // On snappy shell throws below error
       // snappy> select acos(2);
       // ERROR 22003: (SQLState=22003 Severity=20000)
       // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
       // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
       query = "select acos(2)"
       var sparkDf = sparkSession.sql(s"$query")
       var snappyDf = snc.sql(s"$query")
       validateResult(sparkDf, snappyDf)

        query = "SELECT acos(1)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

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

        // without alias throws error
        query = "SELECT acos(2.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
   }

   test("asin"){
       query = "SELECT asin(0)"
       var sparkDf = sparkSession.sql(s"$query")
       var snappyDf = snc.sql(s"$query")
       validateResult(sparkDf, snappyDf)

       // On snappy shell throws below error
       // snappy> SELECT asin(2);
       // ERROR 22003: (SQLState=22003 Severity=20000)
       // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0)
       // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
       query = "SELECT asin(2)"
       sparkDf = sparkSession.sql(s"$query")
       snappyDf = snc.sql(s"$query")
       validateResult(sparkDf, snappyDf)

       query = "SELECT asin(-2)"
       sparkDf = sparkSession.sql(s"$query")
       snappyDf = snc.sql(s"$query")
       validateResult(sparkDf, snappyDf)

       query = "SELECT asin(null)"
       sparkDf = sparkSession.sql(s"$query")
       snappyDf = snc.sql(s"$query")
       validateResult(sparkDf, snappyDf)

       // without alias throws error
       query = "SELECT asin(2.2) as res"
       sparkDf = sparkSession.sql(s"$query")
       snappyDf = snc.sql(s"$query")
       validateResult(sparkDf, snappyDf)
   }

   test("atan"){
       query = "SELECT atan(0)"
       var sparkDf = sparkSession.sql(s"$query")
       var snappyDf = snc.sql(s"$query")
       validateResult(sparkDf, snappyDf)

       query = "SELECT atan(2)"
       sparkDf = sparkSession.sql(s"$query")
       snappyDf = snc.sql(s"$query")
       validateResult(sparkDf, snappyDf)

       query = "SELECT atan(-2)"
       sparkDf = sparkSession.sql(s"$query")
       snappyDf = snc.sql(s"$query")
       validateResult(sparkDf, snappyDf)

       query = "SELECT atan(null)"
       sparkDf = sparkSession.sql(s"$query")
       snappyDf = snc.sql(s"$query")
       validateResult(sparkDf, snappyDf)

       // without alias throws error
       query = "SELECT atan(2.2) as res"
       sparkDf = sparkSession.sql(s"$query")
       snappyDf = snc.sql(s"$query")
       validateResult(sparkDf, snappyDf)
   }

    test("atan2"){
        query = "SELECT atan2(0, 0)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT atan2(2, 3)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT atan2(2, null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT atan2(2.2, 3) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("bin"){
        query = "SELECT bin(13)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT bin(-13)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT bin(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error in test
        query = "SELECT bin(13.3) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("bround"){

        // without alias throws errors for below queries
        query = "SELECT bround(2.5, 0) as res"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT bround(2.5, 3) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT bround(2.5, null) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT round(0, null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("cbrt"){

        query = "SELECT cbrt(25)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT cbrt(0)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT cbrt(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws errors for below query
        query = "SELECT cbrt(27.0) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("ceil"){
        // without alias throws errors for below query
        query = "SELECT ceil(-0.1) as res"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT ceil(5)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT ceil(0)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT ceil(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("ceiling"){
        // without alias throws error for below query
        query = "SELECT ceiling(-0.1) as res"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT ceiling(5)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT ceiling(0)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT ceiling(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("cos"){

        query = "SELECT cos(0)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT cos(2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT cos(-2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT cos(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT cos(2.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("cosh"){

        query = "SELECT cosh(0)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT cosh(2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT cosh(-2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT cosh(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT cosh(2.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("conv"){

        // without alias throws errors for below queries
        query = "SELECT conv('100', 2, 10) as res"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT conv(-10, 16, -10) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("degrees"){
        // without alias throws errors for below queries
        query = "SELECT degrees(3.141592653589793) as res"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT degrees(6.283185307179586 ) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT degrees(null) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT degrees(0) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("e"){
        query = "SELECT e()"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("exp"){
        query = "SELECT exp(0)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT exp(2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT exp(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("expm1"){
        query = "SELECT expm1(0)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT expm1(2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT expm1(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("floor"){

        query = "SELECT floor(5)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT floor(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT floor(0)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias below query only throws error
        query = "SELECT floor(-0.1) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("factorial"){
        query = "SELECT factorial(5)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT factorial(-5)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT factorial(0)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT factorial(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("hex"){

        query = "SELECT hex(17)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT hex(0)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT hex(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias below query throws error
        query = "SELECT hex('Spark SQL') as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)


    }

    test("hypot"){

        query = "SELECT hypot(3, 4)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT hypot(7,8)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

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

    test("log"){

        query = "SELECT log(10, 100)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT log(10,1000)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT log(10,0)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT log(10,null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT log(10, 1000.234) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("log1p"){

        query = "SELECT log1p(0)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT log1p(2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT log1p(-2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias below query throws error
        query = "SELECT log1p(1.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT log1p(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("log2"){
        query = "SELECT log2(0)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT log2(2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT log2(-2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias below query throws error
        query = "SELECT log2(1.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT log2(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("ln"){
        query = "SELECT ln(0)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT ln(1)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT ln(-1)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias below query throws error
        query = "SELECT ln(1.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT ln(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("negative"){
        query = "SELECT negative(0)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT negative(1)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT negative(-1)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT negative(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias below both query throws error
        query = "SELECT negative(1.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT negative(-1.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

    }

    test("pi"){
        query = "SELECT pi()"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("pmod"){
        query = "SELECT pmod(10,3)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT pmod(-10,3)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT pmod(0,3)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT pmod(null,3)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias below query throws error
        query = "SELECT pmod(1.2,3) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("positive"){
        query = "SELECT positive(0)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT positive(1)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT positive(-1)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT positive(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias below both query throws error
        query = "SELECT positive(1.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT positive(-1.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

    }

    test("pow"){
        query = "SELECT pow(3,2)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT pow(-10,3)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT pow(0,3)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT pow(null,3)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias below query throws error
        query = "SELECT pow(1.2,3) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("power"){
        query = "SELECT power(3,2)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT power(-10,3)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT power(0,3)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT power(null,3)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias below query throws error
        query = "SELECT power(1.2,3) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("radians"){

        // without alias only below query throws error
        query = "SELECT radians(360.0) as res"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT radians(180)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT radians(0)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT radians(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("rint"){
        query = "SELECT rint(12.3456) as res"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT rint(-12.3456) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

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

    test("round"){

        query = "SELECT round(2.5, 0) as res"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT round(2.5, 3) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT round(2.5, null) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT round(0, null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("shiftleft"){

        query = "SELECT shiftleft(4, 1)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT shiftleft(0, 1)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT shiftleft(null, null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT shiftleft(2.2, 2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT shiftleft(2.2, 0) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("shiftright"){

        query = "SELECT shiftright(4, 1)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT shiftright(0, 1)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT shiftright(null, null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT shiftright(2.2, 2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT shiftright(2.2, 0) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("shiftrightunsigned"){

        query = "SELECT shiftrightunsigned(4, 1)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT shiftrightunsigned(0, 1)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT shiftrightunsigned(null, null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT shiftrightunsigned(2.2, 2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT shiftrightunsigned(2.2, 0) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("sign"){
        query = "SELECT sign(40)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT sign(-40)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT sign(0)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT sign(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT sign(-4.20) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("signum"){
        query = "SELECT signum(40)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT signum(-40)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT signum(0)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT signum(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT signum(-4.20) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("sin"){
        query = "SELECT sin(0)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT sin(2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT sin(-2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT sin(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT sin(2.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("sinh"){
        query = "SELECT sinh(0)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT sinh(2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT sinh(-2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT sinh(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT sinh(2.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("str_to_map"){

        // throws below error without alias
        // cannot resolve '`str_to_map(a, ,, :)`' given input columns: [str_to_map('a', ,, :)];;
        // and after providing alias throws another error
        // Cannot have map type columns in DataFrame which calls set
        // operations(intersect, except, etc.), but the type of column res is map<string,string>;;
       query = "SELECT str_to_map('a:1,b:2,c:3', ',', ':') as res"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT str_to_map('a')"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT str_to_map('-1.2:a')"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT str_to_map(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("sqrt"){

        query = "SELECT sqrt(4)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // On snappy shell throws below error for this query
        // snappy> select sqrt(-4);
        // ERROR 22003: (SQLState=22003 Severity=20000)
        // (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-1)
        // The resulting value is outside the range for data type 'DOUBLE' column 'null'.
        query = "SELECT sqrt(-4)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT sqrt(0)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT sqrt(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT sqrt(4.4) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("tan"){

        query = "SELECT tan(0)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT tan(2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT tan(-2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT tan(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT tan(2.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT tan(-2.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("tanh"){

        query = "SELECT tanh(0)"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT tanh(2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT tanh(-2)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT tanh(null)"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT tanh(2.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT tanh(-2.2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("+"){
        query = "SELECT (1+1)+3"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT 1.2+3+(4.5+2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT 0+0"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT 0+null"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("-"){
        query = "SELECT 1-1-1"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT 0-0"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT 0-null"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT 1.2-3-(4.5-2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("*"){
        query = "SELECT 4*2"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT 0*0"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT 0*null"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT 1.2*3*(4.5*2) as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("/"){
        query = "SELECT 4/2"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT 0/0"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT 0/null"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT 4.5/2 as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("%"){
        query = "SELECT 4%2"
        var sparkDf = sparkSession.sql(s"$query")
        var snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT 0%0"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        query = "SELECT 0%null"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)

        // without alias throws error
        query = "SELECT 4.5%2 as res"
        sparkDf = sparkSession.sql(s"$query")
        snappyDf = snc.sql(s"$query")
        validateResult(sparkDf, snappyDf)
    }

    test("avg"){
        var sparkQuery = "SELECT avg(intcol) from sparktable"
        var snappyQuery = "SELECT avg(intcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "avg_q1", "RowTable", pw, sparkSession)
        /* var sparkDf = sparkSession.sql(s"$sparkQuery")
        var snappyDf = snc.sql(s"$snappyQuery")
        var sparkDf1 = sparkDf
        validateResult(sparkDf1, snappyDf) */

        sparkQuery = "SELECT avg(intcol) from sparktable"
        snappyQuery = "SELECT avg(intcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "avg_q2", "ColumnTable", pw, sparkSession)
    }

    test("count"){
        var sparkQuery = "SELECT count(*) from sparktable"
        var snappyQuery = "SELECT count(*) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "count_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT count(intcol) from sparktable"
        snappyQuery = "SELECT count(intcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "count_q2", "RowTable", pw, sparkSession)

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

    test("first"){
        var sparkQuery = "SELECT first(stringcol) from sparktable"
        var snappyQuery = "SELECT first(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "first_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT first(stringcol, true) from sparktable"
        snappyQuery = "SELECT first(stringcol, true) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "first_q2", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT first(stringcol) from sparktable"
        snappyQuery = "SELECT first(stringcol) from columntable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "first_q3", "ColumnTable", pw, sparkSession)

        sparkQuery = "SELECT first(stringcol, true) from sparktable"
        snappyQuery = "SELECT first(stringcol, true) from columntable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "first_q4", "ColumnTable", pw, sparkSession)
    }

    test("first_value"){
        var sparkQuery = "SELECT first_value(stringcol) from sparktable"
        var snappyQuery = "SELECT first_value(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "first_value_q1", "RowTable", pw, sparkSession)

        // throws below error
        //  org.apache.spark.sql.AnalysisException:
        // The second argument of First should be a boolean literal.;;
        sparkQuery = "SELECT first_value(stringcol, true) from sparktable"
        snappyQuery = "SELECT first_value(stringcol, true) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "first_value_q2", "RowTable", pw, sparkSession)

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

    test("last"){
        var sparkQuery = "SELECT last(stringcol) from sparktable"
        var snappyQuery = "SELECT last(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "last_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT last(stringcol, true) from sparktable"
        snappyQuery = "SELECT last(stringcol, true) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "last_q2", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT last(stringcol) from sparktable"
        snappyQuery = "SELECT last(stringcol) from columntable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "last_q3", "ColumnTable", pw, sparkSession)

        sparkQuery = "SELECT last(stringcol, true) from sparktable"
        snappyQuery = "SELECT last(stringcol, true) from columntable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "last_q4", "ColumnTable", pw, sparkSession)
    }

    test("last_value"){
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
        snappyQuery = "SELECT last_value(stringcol, true) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "last_value_q2", "RowTable", pw, sparkSession)

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

    test("max"){
        var sparkQuery = "SELECT max(intcol) from sparktable"
        var snappyQuery = "SELECT max(intcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "max_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT max(intcol) from sparktable"
        snappyQuery = "SELECT max(intcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "max_q2", "ColumnTable", pw, sparkSession)
    }

    test("min"){
        var sparkQuery = "SELECT min(intcol) from sparktable"
        var snappyQuery = "SELECT min(intcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "min_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT min(intcol) from sparktable"
        snappyQuery = "SELECT min(intcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "min_q2", "ColumnTable", pw, sparkSession)
    }

    test("sum"){
        var sparkQuery = "SELECT sum(intcol) from sparktable"
        var snappyQuery = "SELECT sum(intcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "sum_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT sum(intcol) from sparktable"
        snappyQuery = "SELECT sum(intcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "sum_q2", "ColumnTable", pw, sparkSession)
    }

    test("length"){
        var sparkQuery = "SELECT length(stringcol) from sparktable"
        var snappyQuery = "SELECT length(stringcol) from columntable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "length_q1", "ColumnTable", pw, sparkSession)

        sparkQuery = "SELECT length(stringcol) from sparktable"
        snappyQuery = "SELECT length(stringcol) from rowTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "length_q1", "RowTable", pw, sparkSession)

        // without alias throws error
        query = "SELECT length('Spark SQL') as res"
        var sparkDF = sparkSession.sql(s"$query")
        var snappyDF = snc.sql(s"$query")
        validateResult(sparkDF, snappyDF)
    }

    test("lower"){
        var sparkQuery = "SELECT lower(stringcol) from sparktable"
        var snappyQuery = "SELECT lower(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "lower_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT lower(stringcol) from sparktable"
        snappyQuery = "SELECT lower(stringcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "lower_q2", "ColumnTable", pw, sparkSession)

        // without alias throws error
        query = "SELECT lower('Spark SQL') as res"
        var sparkDF = sparkSession.sql(s"$query")
        var snappyDF = snc.sql(s"$query")
        validateResult(sparkDF, snappyDF)

        // without alias throws error
        query = "SELECT lower('abcABC123@#$%^&') as res"
        sparkDF = sparkSession.sql(s"$query")
        snappyDF = snc.sql(s"$query")
        validateResult(sparkDF, snappyDF)

    }

    test("lcase"){
        var sparkQuery = "SELECT lcase(stringcol) from sparktable"
        var snappyQuery = "SELECT lcase(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "lcase_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT lcase(stringcol) from sparktable"
        snappyQuery = "SELECT lcase(stringcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "lcase_q2", "ColumnTable", pw, sparkSession)

        // without alias throws error
        query = "SELECT lcase('Spark SQL') as res"
        var sparkDF = sparkSession.sql(s"$query")
        var snappyDF = snc.sql(s"$query")
        validateResult(sparkDF, snappyDF)

        // without alias throws error
        query = "SELECT lcase('abcABC123@#$%^&') as res"
        sparkDF = sparkSession.sql(s"$query")
        snappyDF = snc.sql(s"$query")
        validateResult(sparkDF, snappyDF)
    }

    test("upper"){
        var sparkQuery = "SELECT upper(stringcol) from sparktable"
        var snappyQuery = "SELECT upper(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "upper_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT upper(stringcol) from sparktable"
        snappyQuery = "SELECT upper(stringcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "upper_q2", "ColumnTable", pw, sparkSession)

        // without alias throws error
        query = "SELECT upper('Spark SQL') as res"
        var sparkDF = sparkSession.sql(s"$query")
        var snappyDF = snc.sql(s"$query")
        validateResult(sparkDF, snappyDF)

        // without alias throws error
        query = "SELECT upper('abcABC123@#$%^&') as res"
        sparkDF = sparkSession.sql(s"$query")
        snappyDF = snc.sql(s"$query")
        validateResult(sparkDF, snappyDF)
    }

    test("ucase"){
        var sparkQuery = "SELECT ucase(stringcol) from sparktable"
        var snappyQuery = "SELECT ucase(stringcol) from rowtable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "ucase_q1", "RowTable", pw, sparkSession)

        sparkQuery = "SELECT ucase(stringcol) from sparktable"
        snappyQuery = "SELECT ucase(stringcol) from columnTable"
        assertQueryFullResultSet(snc, sparkQuery, snappyQuery, 1,
            "ucase_q2", "ColumnTable", pw, sparkSession)

        // without alias throws error
        query = "SELECT ucase('Spark SQL') as res"
        var sparkDF = sparkSession.sql(s"$query")
        var snappyDF = snc.sql(s"$query")
        validateResult(sparkDF, snappyDF)

        // without alias throws error
        query = "SELECT ucase('abcABC123@#$%^&') as res"
        sparkDF = sparkSession.sql(s"$query")
        snappyDF = snc.sql(s"$query")
        validateResult(sparkDF, snappyDF)

    }
    
    
}
