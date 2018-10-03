/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package io.snappydata.hydra.cdcConnector

import java.io.{BufferedReader, File, FileNotFoundException, FileReader, IOException, PrintWriter}
import java.net.InetAddress
import java.sql.{Connection, DriverManager}
import java.util.Properties

import scala.sys.process._

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.{SnappyFunSuite, SnappyTableStatsProviderService}
import org.scalatest.BeforeAndAfterAll
import java.util.ArrayList
import java.sql.ResultSet

import com.mysql.jdbc.Statement
import io.snappydata.cluster.SplitSnappyClusterDUnitTest.{getEnvironmentVariable, logInfo}
import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase}
import io.snappydata.util.TestUtils

import org.apache.spark.sql.collection.Utils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, ParseException, SnappyContext, SnappySession, SparkSession, ThinClientConnectorMode}

class DmlOpsInBatchTest extends  SnappyFunSuite with BeforeAndAfterAll{
    var homeDir = System.getProperty("user.home")

    private val snappyProductDir = s"$homeDir/snappydata/build-artifacts/scala-2.11/snappy"
    private val testDir = s"$homeDir/snappydata/dtests/src/resources/scripts/cdcConnector"

    val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort
    val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE
    val spark: SparkSession = SparkSession
        .builder
        .appName("DmlOpsInBatchTest")
        .master("local[*]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
    protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
        /**
         * Setting local[n] here actually supposed to affect number of reservoir created
         * while sampling.
         *
         * Change of 'n' will influence results if they are dependent on weights - derived
         * from hidden column in sample table.
         */
        new org.apache.spark.SparkConf().setAppName("PreparedQueryRoutingSingleNodeSuite")
            .setMaster("local[4]")
        // .set("spark.logConf", "true")
        // .set("mcast-port", "4958")
    }

    override def beforeAll(): Unit = {

        // System.setProperty("org.codehaus.janino.source_debugging.enable", "true")
        // System.setProperty("spark.testing", "true")
        super.beforeAll()
        // reducing DML chunk size size to force lead node to send
        // results in multiple batches
        setDMLMaxChunkSize(50L)

        // scalastyle:off println
        before()

    }

    override def afterAll(): Unit = {
        // System.clearProperty("org.codehaus.janino.source_debugging.enable")
        // System.clearProperty("spark.testing")
        setDMLMaxChunkSize(default_chunk_size)
        super.afterAll()
        after()

    }

    def setDMLMaxChunkSize(size: Long): Unit = {
        GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
    }
    def before(): Unit = {
        println("product dir" + snappyProductDir)
        (snappyProductDir + "/sbin/snappy-start-all.sh").!!
        println("Started snappy cluster")

        (snappyProductDir + s"/bin/snappy run -file=$testDir/testCases/createTable.sql" +
            s" -path=$testDir/testCases/ " +
            s"-client-bind-address=localhost -client-port=1527").!!
        println("Table created and jar deployed")

        (snappyProductDir + "/bin/snappy-job.sh submit " +
            "--app-name JavaCdcStreamingApp " +
            "--class io.snappydata.app.JavaCdcStreamingApp " +
            s"--conf configFile=$testDir/confFiles/" +
            "source_destination_tables.properties " +
            s"--conf configFile1=$testDir/confFiles/" +
            "cdc_source_connection.properties " +
            s"--app-jar $testDir/cdcConnectorModifiedCode/cdc/target/" +
            "original-cdc-test-0.0.1.jar " +
            "--lead localhost:8090 ").!!
        println("Job started")
    }
    def after(): Unit = {
        (snappyProductDir + "/sbin/snappy-stop-all.sh").!!
        println("Stopped snappy cluster")
    }

    def getConnection: Connection = {

            println("Getting connection")
            // scalastyle:off classforname Class.forName
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
            println("Clas.forname    ")
            val conn = DriverManager.getConnection(
                "jdbc:sqlserver://sqlent.westus.cloudapp.azure.com:1433;DatabaseName=testdatabase",
                "sqldb", "snappydata#msft1")
            println("Got connection" + conn.isClosed)
            conn
    }

    def getQuery(fileName: String): ArrayList[String] = {
        val queryList = new ArrayList[String]
        try{
            var s: String = new String()
            var sb: StringBuffer = new StringBuffer()

            val fr: FileReader = new FileReader(new File(fileName))
            // be sure to not have line starting with ""--"" or
            // ""/*"" or any other non aplhabetical character

            var br: BufferedReader = new BufferedReader(fr)
            s = br.readLine()

            while (s != null) {
                sb.append(s)
                s = br.readLine()
            }
            br.close()

            // here is our splitter ! We use "";"" as a delimiter for each request
            // then we are sure to have well formed statements
            var splitData: Array[String] = sb.toString().split(";")
            for (i <- 0 to splitData.length-1) {
                {
                    if (!(splitData(i) == null) || !(splitData(i).length == 0)) {
                        val qry = splitData(i)
                        println("The query is " + qry)
                        queryList.add(qry)
                    }
                }
            }
        }

        catch {
            case e: FileNotFoundException => {
            }
            case io: IOException => {
            }
        }
        queryList
    }

    def executeQueries(queryArray: ArrayList[String], conn: Connection) {
        try {
            val statement = conn.createStatement()
            for (i <- 0 to queryArray.size()) {
                var query = queryArray.get(i)
                statement.executeUpdate(query)
            }
        }
        catch {
            case e: Exception => {
                System.out.println("Caught exception " + e.getMessage)
            }
        }
    }

    def getANetConnection(netPort: Int): Connection = {
        val driver = "io.snappydata.jdbc.ClientDriver"
        Utils.classForName(driver).newInstance
        DriverManager.getConnection("jdbc:snappydata:thrift://localhost[1527]")
    }
    def performValidation(sqlResultSet: ResultSet, snappyResultSet: ResultSet): Unit = {
        val resultMetaData = sqlResultSet.getMetaData
        val columnCnt = resultMetaData.getColumnCount
        val snappyMetadata = snappyResultSet.getMetaData
        val snappyColCnt = snappyMetadata.getColumnCount
        println("Column cnt of snappy and sql table is = " + snappyColCnt + " " + columnCnt)
        while (sqlResultSet.next()) {
            snappyResultSet.next()
            for (i <- 1 to columnCnt) {
                if (sqlResultSet.getObject(i).equals(snappyResultSet.getObject(i))) {
                    println("match " + sqlResultSet.getObject(i) + " "
                        + snappyResultSet.getObject(i))
                }
                else {
                    println("not match" + sqlResultSet.getObject(i)
                        + " " + snappyResultSet.getObject(i))
                }
            }
        }
    }

    test("insert update delete insert on same key in one batch"){
        
        val queryString = s"$testDir/testCases/testCase1.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 95000010061;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 95000010061;")
        performValidation(sqlResultSet, snappyDF)
        
    }

    test("insert delete insert on same key in one batch"){
        
        val queryString = s"$testDir/testCases/testCase2.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 95000010062;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 95000010062;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("insert delete insert on two keys in one batch"){

        val queryString = s"$testDir/testCases/testCase3.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id >= 95000010063 and adj_id <= 95000010064 order by adj_id;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id >= 95000010063 and adj_id <= 95000010064 order by adj_id;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("insert delete insert update on same key in one batch"){

        val queryString = s"$testDir/testCases/testCase4.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 95000010065;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 95000010065;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("multiple inserts deletes inserts on keys in one batch"){

        val queryString = s"$testDir/testCases/testCase5.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id >= 95000010066 and adj_id <= 95000010067 order by adj_id;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id >= 95000010066 and adj_id <= 95000010067 order by adj_id;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("multiple inserts deletes inserts updates on keys in one batch"){

        val queryString = s"$testDir/testCases/testCase6.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id >= 95000010068 and adj_id <= 95000010069 order by adj_id;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id >= 95000010068 and adj_id <= 95000010069 order by adj_id;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("insert delete insert update on same key having diff lsn in one batch"){

        val queryString = s"$testDir/testCases/testCase7.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 95000010070;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 95000010070;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("insert delete insert on same key having diff lsn in one batch"){

        val queryString = s"$testDir/testCases/testCase8.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 95000010071;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 95000010071;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("multiple inserts deletes inserts on keys having diff lsn in one batch"){

        val queryString = s"$testDir/testCases/testCase9.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id >= 95000010072 adj_id <= 95000010073 order by adj_id;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id >= 95000010072 adj_id <= 95000010073 order by adj_id;")
        performValidation(sqlResultSet, snappyDF)
    }

    test("insert update on same key in one batch"){

        val queryString = s"$testDir/testCases/testCase10.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 95000010074;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 95000010074;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("insert update on same key having diff lsn in one batch"){

        val queryString = s"$testDir/testCases/testCase11.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 95000010075;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 95000010075;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("insert update delete insert update on same key in one batch"){

        val queryString = s"$testDir/testCases/testCase12.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 95000010076;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 95000010076;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("multiple inserts deletes inserts updates on diff keys in one batch"){

        val queryString = s"$testDir/testCases/testCase13.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id >= 95000010077 and adj_id <= 95000010080 order by adj_id;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id >= 95000010077 and adj_id <= 95000010080 order by adj_id;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("multiple inserts updates deletes inserts updates in one batch"){

        val queryString = s"$testDir/testCases/testCase14.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id >= 95000010081 and adj_id <= 95000010084 order by adj_id;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id >= 95000010081 and adj_id <= 95000010084 order by adj_id;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("multiple inserts updates in one batch"){

        val queryString = s"$testDir/testCases/testCase15.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id >= 95000010085 and adj_id <= 95000010088 order by adj_id;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id >= 95000010085 and adj_id <= 95000010088 order by adj_id;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("multiple inserts in one batch"){

        val queryString = s"$testDir/testCases/testCase16.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id >= 95000010089 and adj_id <= 95000010092 order by adj_id;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id >= 95000010089 and adj_id <= 95000010092 order by adj_id;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("single insert in one batch"){

        val queryString = s"$testDir/testCases/testCase17.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 95000010093;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 95000010093;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("single update in one batch"){

        val queryString = s"$testDir/testCases/testCase18.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 95000010093;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 95000010093;")
        performValidation(sqlResultSet, snappyDF)

    }

    test("single delete in one batch"){

        val queryString = s"$testDir/testCases/testCase19.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 95000010093;")
        if (snappyDF.next) System.out.println("FAILURE : The result set should have been empty")
        else System.out.println("SUCCESS : The result set is empty as expected for 95000010093")

    }

    test("multiple inserts deletes in one batch"){

        val queryString = s"$testDir/testCases/testCase20.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id >= 95000010094 and adj_id <= 95000010097 order by adj_id;")
        if (snappyDF.next) System.out.println("FAILURE : The result set should have been empty")
        else System.out.println("SUCCESS : The result set is empty as expected")

    }

    test("multiple inserts updates deletes in one batch"){

        val queryString = s"$testDir/testCases/testCase21.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id >= 95000010098 and adj_id <= 950000100100 order by adj_id;")
        if (snappyDF.next) System.out.println("FAILURE : The result set should have been empty")
        else System.out.println("SUCCESS : The result set is empty as expected")

    }

    test("insert update delete on two diff key in one batch"){

        val queryString = s"$testDir/testCases/testCase22.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id >= 950000100101 and adj_id <= 950000100102 order by adj_id;")
        if (snappyDF.next) System.out.println("FAILURE : The result set should have been empty")
        else System.out.println("SUCCESS : The result set is empty as expected")

    }

    test("insert update delete on diff keys in one batch"){

        val queryString = s"$testDir/testCases/testCase23.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 950000100103;")
        if (snappyDF.next) System.out.println("FAILURE : The result set should have been empty")
        else System.out.println("SUCCESS : The result set is empty as expected for 950000100103")
        val snappyDF1 = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 950000100104;")
        val sqlResultSet1 = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 950000100104;")
        performValidation(sqlResultSet1, snappyDF1)

    }

    test("dml ops in one batch"){

        val queryString = s"$testDir/testCases/testCase24.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 950000100105;")
        if (snappyDF.next) System.out.println("FAILURE : The result set should have been empty")
        else System.out.println("SUCCESS : The result set is empty as expected for 950000100105")
        val snappyDF1 = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 950000100106;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 950000100106;")
        performValidation(sqlResultSet, snappyDF1)

    }

    test("dml ops on diff keys in one batch"){

        val queryString = s"$testDir/testCases/testCase25.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 950000100107;")
        if (snappyDF.next) System.out.println("FAILURE : The result set should have been empty")
        else System.out.println("SUCCESS : The result set is empty as expected for 950000100107")
        val snappyDF1 = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 950000100108;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 950000100108;")
        performValidation(sqlResultSet, snappyDF1)

    }

    test("insert and multiple updates on same key in one batch"){

        val queryString = s"$testDir/testCases/testCase26.sql"
        val qArr = getQuery(queryString)
        val conn = getConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(50000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 950000100109;")
        val sqlResultSet = conn.createStatement().
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 950000100109;")
        performValidation(sqlResultSet, snappyDF)
        
    }
}