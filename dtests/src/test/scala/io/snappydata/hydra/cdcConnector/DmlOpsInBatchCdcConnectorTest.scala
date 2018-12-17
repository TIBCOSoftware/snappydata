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

import java.io.{BufferedReader, File, FileNotFoundException, FileReader, IOException}
import java.sql.{Connection, DriverManager, ResultSet}
import java.util
import java.util.ArrayList
import scala.sys.process._
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import hydra.Log
import io.snappydata.hydra.SnappyHydraTestRunner
import io.snappydata.test.dunit.AvailablePortHelper
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{IOFileFilter, TrueFileFilter, WildcardFileFilter}
import org.apache.spark.sql.collection.Utils

class DmlOpsInBatchCdcConnectorTest extends SnappyHydraTestRunner {

    Log.createLogWriter("TestCdcConnector", "info")
    private var testDir = ""
    private var appJar = ""
    var homeDir = ""

    val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort

    override def beforeAll(): Unit = {
        snappyHome = System.getenv("SNAPPY_HOME")
        if (snappyHome == null) {
            throw new Exception("SNAPPY_HOME should be set as an environment variable")
        }
        homeDir = System.getProperty("user.home")
        testDir = s"$snappyHome/../../../dtests/src/resources/scripts/cdcConnector"
        appJar = s"$snappyHome/../../../snappy-poc/cdc/target/cdc-test-0.0.1.jar"
        Log.getLogWriter.info("Snappy home : " + snappyHome)
        Log.getLogWriter.info("Home Dir : " + homeDir)
        Log.getLogWriter.info("Test Config Dir : " + testDir)
        Log.getLogWriter.info("App Jar location : " + appJar)

        setDMLMaxChunkSize(50L)
        before()

    }

    override def afterAll(): Unit = {
        after()

    }

    def setDMLMaxChunkSize(size: Long): Unit = {
        GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
    }

    protected def getUserAppJarLocation(jarName: String, jarPath: String): String = {
        var userAppJarPath: String = null
        if (new File(jarName).exists) jarName
        else {
            val baseDir: File = new File(jarPath)
            try {
                val filter: IOFileFilter = new WildcardFileFilter(jarName)
                val files: util.List[File] = FileUtils.listFiles(baseDir, filter,
                    TrueFileFilter.INSTANCE).asInstanceOf[util.List[File]]
                Log.getLogWriter.info("Jar file found: " + util.Arrays.asList(files))
                import scala.collection.JavaConverters._
                for (file1: File <- files.asScala) {
                    if (!file1.getAbsolutePath.contains("/work/") ||
                        !file1.getAbsolutePath.contains("/scala-2.10/")) {
                        userAppJarPath = file1.getAbsolutePath
                    }

                }
            } catch {
                case e: Exception =>
                    Log.getLogWriter.info("Unable to find " + jarName
                        + " jar at " + jarPath + " location.")
            }
            userAppJarPath
        }
    }

    def truncateSqlTable(): Unit = {

        // scalastyle:off classforname Class.forName

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
        val conn = DriverManager.getConnection(
            "jdbc:sqlserver://sqlent.westus.cloudapp.azure.com:1433;DatabaseName=testdatabase",
            "sqldb", "snappydata#msft1")
        conn.createStatement().execute("delete from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where (adj_id between 95000010061 and 950000100110)")
        Thread.sleep(50000)
        conn.createStatement().execute("truncate table testdatabase.cdc.dbo_ADJUSTMENT_CT")

        val sqlDF = conn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
            .executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where (adj_id between 95000010061 and 950000100110)")

        if (sqlDF.next) Log.getLogWriter.info("FAILURE : The result set should have been empty")
        else Log.getLogWriter.info("SUCCESS : The result set is empty as expected")

        Log.getLogWriter.info("Deleted rows from key 95000010061 to 950000100110")
        Log.getLogWriter.info("Truncated cdc table")
    }

    def before(): Unit = {
        truncateSqlTable()

        (snappyHome + "/sbin/snappy-start-all.sh").!!
        Log.getLogWriter.info("Started snappy cluster")

        var filePath = s"$testDir/testCases/createTable.sql"
        var dataLocation = s"$snappyHome/../../../snappy-connectors/" +
            s"jdbc-stream-connector/build-artifacts/scala-2.11/libs"
        val connectorJar = getUserAppJarLocation("snappydata-jdbc-stream-connector_2.11-0.9.3.jar",
            dataLocation)

        val command = "/bin/snappy run -file=" + filePath +
            " -param:dataLocation=" + connectorJar + " -param:homeDirLocation=" + snappyHome +
            " -client-port=1527" + " -client-bind-address=localhost"

        (snappyHome + command).!!

        Log.getLogWriter.info("Table created and jar deployed")
        Log.getLogWriter.info("appJar " + appJar)
        val jobCommand = "/bin/snappy-job.sh submit " +
            "--app-name JavaCdcStreamingAppSnappyJob " +
            "--class io.snappydata.app.JavaCdcStreamingAppSnappyJob " +
            s"--conf configFile=$testDir/confFiles/" +
            "source_destination_tables.properties " +
            s"--conf configFile1=$testDir/confFiles/" +
            "cdc_source_connection.properties " +
            s"--app-jar $appJar " +
            "--lead localhost:8090"

        (snappyHome + jobCommand).!!
        Log.getLogWriter.info("Job started")
    }

    def after(): Unit = {
        (snappyHome + "/sbin/snappy-stop-all.sh").!!
        Log.getLogWriter.info("Stopped snappy cluster")
    }

    def getSqlServerConnection: Connection = {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
        val conn = DriverManager.getConnection(
            "jdbc:sqlserver://sqlent.westus.cloudapp.azure.com:1433;DatabaseName=testdatabase",
            "sqldb", "snappydata#msft1")
        conn
    }

    def getQuery(fileName: String): ArrayList[String] = {
        val queryList = new ArrayList[String]
        try {
            var s: String = new String()
            var sb: StringBuffer = new StringBuffer()

            val fr: FileReader = new FileReader(new File(fileName))
            var br: BufferedReader = new BufferedReader(fr)
            s = br.readLine()

            while (s != null) {
                sb.append(s)
                s = br.readLine()
            }
            br.close()
            var splitData: Array[String] = sb.toString().split(";")
            for (i <- 0 to splitData.length - 1) {
                {
                    if (!(splitData(i) == null) || !(splitData(i).length == 0)) {
                        val qry = splitData(i)
                        Log.getLogWriter.info("The query is " + qry)
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

    def executeQueries(queryArray: ArrayList[String], conn: Connection): Unit = {

        val statement = conn.createStatement()
        for (i <- 0 until queryArray.size()) {
            var query = queryArray.get(i)
            statement.executeUpdate(query)
        }
    }

    def getANetConnection(netPort: Int): Connection = {
        val driver = "io.snappydata.jdbc.ClientDriver"
        Utils.classForName(driver).newInstance
        DriverManager.getConnection("jdbc:snappydata:thrift://localhost[1527]")
    }

    def collectData(queryPath: String, query1: String,
        snappyQuery: String, sqlQuery: String): Unit = {

        Log.getLogWriter.info("Execute queries and print results")
        val qArr = getQuery(queryPath)
        val conn = getSqlServerConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        // Thread.sleep(20000)
        var rs = snappyconn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(query1)
        while (!rs.next()) {
            rs = snappyconn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
                executeQuery(query1)
        }
        val snappyDF = snappyconn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
            .executeQuery(snappyQuery)
        val sqlResultSet = conn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
            .executeQuery(sqlQuery)
        performValidation(sqlResultSet, snappyDF)

    }

    def performValidation(sqlResultSet: ResultSet, snappyResultSet: ResultSet): Unit = {

        val resultMetaData = sqlResultSet.getMetaData
        val columnCnt = resultMetaData.getColumnCount
        val snappyMetadata = snappyResultSet.getMetaData
        val snappyColCnt = snappyMetadata.getColumnCount
        var sqlRowsCnt = 0
        while (sqlResultSet.next()) {
            sqlRowsCnt = sqlRowsCnt + 1
        }
        var snappyRowsCnt = 0
        while (snappyResultSet.next()) {
            snappyRowsCnt = snappyRowsCnt + 1
        }
        sqlResultSet.beforeFirst()
        snappyResultSet.beforeFirst()
        Log.getLogWriter.info("Row cnt of snappy and sql table is = "
            + snappyRowsCnt + " " + sqlRowsCnt)
        Log.getLogWriter.info("Column cnt of snappy and sql table is = "
            + snappyColCnt + " " + columnCnt)
        if (snappyRowsCnt.equals(sqlRowsCnt)) {
            while (sqlResultSet.next()) {
                snappyResultSet.next()
                for (i <- 1 to columnCnt) {
                    if (sqlResultSet.getObject(i).equals(snappyResultSet.getObject(i))) {
                        Log.getLogWriter.info("match " + sqlResultSet.getObject(i) + " "
                            + snappyResultSet.getObject(i))
                    }
                    else {
                        throw new Exception("not match" + sqlResultSet.getObject(i)
                            + " " + snappyResultSet.getObject(i))
                    }
                }
            }
        }
        else {
            throw new Exception("Row cnt of snappy and sql table is = "
                + snappyRowsCnt + " " + sqlRowsCnt + " not matching")
        }
    }

    test("Test for insert,update,delete and insert happening " +
        "sequentially on same key in one batch") {

        val queryString = s"$testDir/testCases/testCase1.sql"
        val query1 = "select descr from ADJUSTMENT where adj_id = 95000010061 and descr = 'iTT';"
        val snQuery = "select * from ADJUSTMENT where adj_id = 95000010061;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] where adj_id = 95000010061;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for insert,delete and insert happening sequentially on same key in one batch") {

        val queryString = s"$testDir/testCases/testCase2.sql"
        val query1 = "select descr from ADJUSTMENT where adj_id = 95000010062 and descr = 'iTT';"
        val snQuery = "select * from ADJUSTMENT where adj_id = 95000010062;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] where adj_id = 95000010062;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for insert,delete and insert happening sequentially on two keys in one batch") {

        val queryString = s"$testDir/testCases/testCase3.sql"
        val query1 = "select descr from ADJUSTMENT where (adj_id between 95000010063" +
            " and 95000010064) and descr = 'iTT';"
        val snQuery = "select * from ADJUSTMENT " +
            "where (adj_id between 95000010063 and 95000010064) order by adj_id;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where (adj_id between 95000010063 and 95000010064) order by adj_id;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for insert,delete,insert and update " +
        "happening sequentially on same key in one batch") {

        val queryString = s"$testDir/testCases/testCase4.sql"
        val query1 = "select descr from ADJUSTMENT where adj_id = 95000010065 and descr = 'iTTT';"
        val snQuery = "select * from ADJUSTMENT where adj_id = 95000010065;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where adj_id = 95000010065;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for multiple inserts,deletes and " +
        "inserts happening sequentially on keys in one batch") {

        val queryString = s"$testDir/testCases/testCase5.sql"
        val query1 = "select descr from ADJUSTMENT where (adj_id between 95000010066" +
            " and 95000010067) and descr = 'iTT';"
        val snQuery = "select * from ADJUSTMENT " +
            "where (adj_id between 95000010066 and 95000010067) order by adj_id;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where (adj_id between 95000010066 and 95000010067) order by adj_id;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for multiple inserts,deletes,inserts " +
        "and updates happening sequentially on keys in one batch") {

        val queryString = s"$testDir/testCases/testCase6.sql"
        val query1 = "select descr from ADJUSTMENT where (adj_id between 95000010068" +
            " and 95000010069) and descr = 'iTTT';"
        var snQuery = "select * from ADJUSTMENT " +
            "where (adj_id between 95000010068 and 95000010069) order by adj_id;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where (adj_id between 95000010068 and 95000010069) order by adj_id;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for insert,delete,insert and update " +
        "happening sequentially on same key having diff lsn in one batch") {

        val queryString = s"$testDir/testCases/testCase7.sql"
        val query1 = "select descr from ADJUSTMENT where adj_id = 95000010070 and descr = 'iTTT';"
        var snQuery = "select * from ADJUSTMENT " +
            "where adj_id = 95000010070;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where adj_id = 95000010070;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for insert,delete and insert happening " +
        "sequentially on same key having diff lsn in one batch") {

        val queryString = s"$testDir/testCases/testCase8.sql"
        val query1 = "select descr from ADJUSTMENT where adj_id = 95000010071 and descr = 'iTT';"
        var snQuery = "select * from ADJUSTMENT " +
            "where adj_id = 95000010071;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where adj_id = 95000010071;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for multiple inserts,deletes and inserts " +
        "happening sequentially on keys having diff lsn in one batch") {

        val queryString = s"$testDir/testCases/testCase9.sql"
        val query1 = "select descr from ADJUSTMENT where (adj_id between 95000010072" +
            " and 95000010073) and descr = 'iTT';"
        var snQuery = "select * from ADJUSTMENT " +
            "where (adj_id between 95000010072 and 95000010073) order by adj_id;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where (adj_id between 95000010072 and 95000010073) order by adj_id;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for insert and update happening sequentially on same key in one batch") {

        val queryString = s"$testDir/testCases/testCase10.sql"
        val query1 = "select descr from ADJUSTMENT where adj_id = 95000010074 and descr = 'iTTT';"
        val snQuery = "select * from ADJUSTMENT " +
            "where adj_id = 95000010074;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where adj_id = 95000010074;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for insert and update happening sequentially" +
        " on same key having diff lsn in one batch") {

        val queryString = s"$testDir/testCases/testCase11.sql"
        val query1 = "select descr from ADJUSTMENT where adj_id = 95000010075 and descr = 'iTTT';"
        var snQuery = "select * from ADJUSTMENT " +
            "where adj_id = 95000010075;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where adj_id = 95000010075;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for insert update,delete,insert and " +
        "update happening sequentially on same key in one batch") {

        val queryString = s"$testDir/testCases/testCase12.sql"
        val query1 = "select descr from ADJUSTMENT where adj_id = 95000010076 and descr = 'iTTTy';"
        var snQuery = "select * from ADJUSTMENT " +
            "where adj_id = 95000010076;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where adj_id = 95000010076;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for multiple inserts,deletes,inserts and " +
        "updates happening sequentially on diff keys in one batch") {

        val queryString = s"$testDir/testCases/testCase13.sql"
        val query1 = "select descr from ADJUSTMENT where (adj_id between 95000010077" +
            " and 95000010080) and descr = 'iTTTs';"
        var snQuery = "select * from ADJUSTMENT " +
            "where (adj_id between 95000010077 and 95000010080) order by adj_id;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where (adj_id between 95000010077 and 95000010080) order by adj_id;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for multiple inserts,updates,deletes,inserts " +
        "and updates happening sequentially in one batch") {

        val queryString = s"$testDir/testCases/testCase14.sql"
        val query1 = "select descr from ADJUSTMENT where (adj_id between 95000010081" +
            " and 95000010084) and descr = 'iTTTs';"
        var snQuery = "select * from ADJUSTMENT " +
            "where (adj_id between 95000010081 and 95000010084) order by adj_id;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where (adj_id between 95000010081 and 95000010084) order by adj_id;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for multiple inserts and updates happening sequentially in one batch") {

        val queryString = s"$testDir/testCases/testCase15.sql"
        val query1 = "select descr from ADJUSTMENT where (adj_id between 95000010085" +
            " and 95000010088) and descr = 'iTTTz';"
        var snQuery = "select * from ADJUSTMENT " +
            "where (adj_id between 95000010085 and 95000010088) order by adj_id;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where (adj_id between 95000010085 and 95000010088) order by adj_id;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for multiple inserts happening sequentially in one batch") {

        val queryString = s"$testDir/testCases/testCase16.sql"
        val query1 = "select descr from ADJUSTMENT where (adj_id between 95000010089" +
            " and 95000010092) and descr = 'iT';"
        var snQuery = "select * from ADJUSTMENT " +
            "where (adj_id between 95000010089 and 95000010092) order by adj_id;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where (adj_id between 95000010089 and 95000010092) order by adj_id;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for single insert in one batch") {

        val queryString = s"$testDir/testCases/testCase17.sql"
        val query1 = "select descr from ADJUSTMENT where adj_id = 95000010093 and descr = 'iT';"
        var snQuery = "select * from ADJUSTMENT " +
            "where adj_id = 95000010093;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where adj_id = 95000010093;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for single update in one batch") {

        val queryString = s"$testDir/testCases/testCase18.sql"
        val query1 = "select descr from ADJUSTMENT where adj_id = 95000010093 and descr = 'iTTTzT';"
        var snQuery = "select * from ADJUSTMENT " +
            "where adj_id = 95000010093;"
        val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT] " +
            "where adj_id = 95000010093;"

        collectData(queryString, query1, snQuery, sqlQuery)
    }

    test("Test for single delete in one batch") {

        val queryString = s"$testDir/testCases/testCase19.sql"
        val qArr = getQuery(queryString)
        val conn = getSqlServerConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(20000)
        val snappyDF = snappyconn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
            .executeQuery("select * from ADJUSTMENT " +
                "where adj_id = 95000010093;")
        if (snappyDF.next) Log.getLogWriter.info("FAILURE : The result set should have been empty")
        else Log.getLogWriter.info("SUCCESS : The result set is empty as expected for 95000010093")

    }

    test("Test for multiple inserts and deletes happening sequentially in one batch") {

        val queryString = s"$testDir/testCases/testCase20.sql"
        val qArr = getQuery(queryString)
        val conn = getSqlServerConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(20000)
        val snappyDF = snappyconn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
            .executeQuery("select * from ADJUSTMENT " +
                "where (adj_id between 95000010094 and 95000010097) order by adj_id;")
        if (snappyDF.next) Log.getLogWriter.info("FAILURE : The result set should have been empty")
        else Log.getLogWriter.info("SUCCESS : The result set is empty as expected")

    }

    test("Test for multiple inserts,updates and deletes happening sequentially in one batch") {

        val queryString = s"$testDir/testCases/testCase21.sql"
        val qArr = getQuery(queryString)
        val conn = getSqlServerConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(20000)
        val snappyDF = snappyconn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
            .executeQuery("select * from ADJUSTMENT " +
                "where (adj_id between 95000010098 and 950000100100) order by adj_id;")
        if (snappyDF.next) Log.getLogWriter.info("FAILURE : The result set should have been empty")
        else Log.getLogWriter.info("SUCCESS : The result set is empty as expected")

    }

    test("Test for insert,update and delete happening sequentially on two diff key in one batch") {

        val queryString = s"$testDir/testCases/testCase22.sql"
        val qArr = getQuery(queryString)
        val conn = getSqlServerConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(20000)
        val snappyDF = snappyconn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
            .executeQuery("select * from ADJUSTMENT " +
                "where (adj_id between 950000100101 and 950000100102) order by adj_id;")
        if (snappyDF.next) Log.getLogWriter.info("FAILURE : The result set should have been empty")
        else Log.getLogWriter.info("SUCCESS : The result set is empty as expected")

    }

    test("Test for insert,update and delete happening sequentially on diff keys in one batch") {

        val queryString = s"$testDir/testCases/testCase23.sql"
        val qArr = getQuery(queryString)
        val conn = getSqlServerConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        // Thread.sleep(35000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 950000100103;")
        if (snappyDF.next) Log.getLogWriter.info("FAILURE : The result set should have been empty")
        else Log.getLogWriter.info("SUCCESS : The result set is empty as expected for 950000100103")
        val query = "select descr from ADJUSTMENT where adj_id = 950000100104 and descr = 'iT';"
        var rs = snappyconn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(query)
        while (!rs.next()) {
            rs = snappyconn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
                executeQuery(query)
        }
        val snappyDF1 = snappyconn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
            .executeQuery("select * from ADJUSTMENT " +
                "where adj_id = 950000100104;")
        val sqlResultSet1 = conn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 950000100104;")
        performValidation(sqlResultSet1, snappyDF1)

    }

    test("Test for dml ops happening sequentially in one batch") {

        val queryString = s"$testDir/testCases/testCase24.sql"
        val qArr = getQuery(queryString)
        val conn = getSqlServerConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(20000)
        val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT " +
            "where adj_id = 950000100105;")
        if (snappyDF.next) Log.getLogWriter.info("FAILURE : The result set should have been empty")
        else Log.getLogWriter.info("SUCCESS : The result set is empty as expected for 950000100105")
        val query = "select descr from ADJUSTMENT where adj_id = 950000100106 and descr = 'iTTTz';"
        var rs = snappyconn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(query)
        while (!rs.next()) {
            rs = snappyconn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
                executeQuery(query)
        }
        val snappyDF1 = snappyconn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
            .executeQuery("select * from ADJUSTMENT " +
                "where adj_id = 950000100106;")
        val sqlResultSet = conn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 950000100106;")
        performValidation(sqlResultSet, snappyDF1)

    }

    test("Test for dml ops happening sequentially on diff keys in one batch") {

        val queryString = s"$testDir/testCases/testCase25.sql"
        val qArr = getQuery(queryString)
        val conn = getSqlServerConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        Thread.sleep(20000)
        val snappyDF = snappyconn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
            .executeQuery("select * from ADJUSTMENT " +
                "where adj_id = 950000100107;")
        if (snappyDF.next) Log.getLogWriter.info("FAILURE : The result set should have been empty")
        else Log.getLogWriter.info("SUCCESS : The result set is empty as expected for 950000100107")
        val query = "select descr from ADJUSTMENT where adj_id = 950000100108 and descr = 'iTTTzy';"
        var rs = snappyconn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(query)
        while (!rs.next()) {
            rs = snappyconn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
                executeQuery(query)
        }
        val snappyDF1 = snappyconn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
            .executeQuery("select * from ADJUSTMENT " +
                "where adj_id = 950000100108;")
        val sqlResultSet = conn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 950000100108;")
        performValidation(sqlResultSet, snappyDF1)

    }

    test("Test for insert and multiple updates happening sequentially on same key in one batch") {

        val queryString = s"$testDir/testCases/testCase26.sql"
        val qArr = getQuery(queryString)
        val conn = getSqlServerConnection
        val snappyconn = getANetConnection(1527)
        executeQueries(qArr, conn)
        // Thread.sleep(35000)
        val query = "select descr from ADJUSTMENT where adj_id = 950000100109" +
            " and POST_DT = '2016-05-10' and DESCR = 'iTTyt'" +
            " and SRC_SYS_REF_ID = '90' and SRC_SYS_REC_ID = '890';"
        var rs = snappyconn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(query)
        while (!rs.next()) {
            rs = snappyconn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
                executeQuery(query)
        }
        val snappyDF = snappyconn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
            .executeQuery("select * from ADJUSTMENT " +
                "where adj_id = 950000100109;")
        val sqlResultSet = conn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
            executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT] " +
                "where adj_id = 950000100109;")
        performValidation(sqlResultSet, snappyDF)

    }
}