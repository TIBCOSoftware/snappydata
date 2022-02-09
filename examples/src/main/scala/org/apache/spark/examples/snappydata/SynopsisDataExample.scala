package org.apache.spark.examples.snappydata

import java.io.{File, PrintWriter}

import scala.util.Try

import com.typesafe.config.Config

import org.apache.spark.sql.{SnappyJobInvalid, SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession, SparkSession}

/**
 * An example that shows how to create sample tables in SnappyData
 * and execute approaximate queries using Synopsis Data Engine feature.
 *
 * Refer to http://tibcosoftware.github.io/snappydata/aqp/ for more details on
 * Synopsis Data Engine
 *
 * <p></p>
 * This example can be run either in local mode(in which it will spawn a single
 * node SnappyData system) or can be submitted as a job to an already running
 * SnappyData cluster.
 *
 * To run the example in local mode go to you SnappyData product distribution
 * directory and type following command on the command prompt
 * <pre>
 * bin/run-example snappydata.SynopsisDataExample quickstart/data
 * </pre>
 *
 * To submit this example as a job to an already running cluster
 * <pre>
 *   cd $SNAPPY_HOME
 *   bin/snappy-job.sh submit
 *   --app-name SynopsisDataExample
 *   --class org.apache.spark.examples.snappydata.SynopsisDataExample
 *   --app-jar examples/jars/quickstart.jar
 *   --lead [leadHost:port]
 *   --conf data_resource_folder=../../quickstart/data
 *
 * Check the status of your job id
 * bin/snappy-job.sh status --lead [leadHost:port] --job-id [job-id]
 *
 * The output of the job will be redirected to a file named SynopsisDataExample.out
 */
object SynopsisDataExample extends SnappySQLJob {

  private var dataFolder: String = ""
  def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    val pw = new PrintWriter("SynopsisDataExample.out")
    dataFolder = s"${jobConfig.getString("data_resource_folder")}"
    runSynopsisDataExample(snappySession, pw)
    pw.close()
    s"Check ${getCurrentDirectory}/SynopsisDataExample.out file for output of this job"
  }

  override def isValidJob(snappySession: SnappySession, config: Config): SnappyJobValidation = {
    {
      Try(config.getString("data_resource_folder"))
          .map(x => SnappyJobValid())
          .getOrElse(SnappyJobInvalid("No data_resource_folder config param"))
    }
  }

  def runSynopsisDataExample(snSession: SnappySession, pw: PrintWriter): Unit = {
    pw.println("****Synopsis Data Example****")
    snSession.sql("DROP TABLE IF EXISTS STAGING_AIRLINE")
    snSession.sql("DROP TABLE IF EXISTS AIRLINE_SAMPLE")
    snSession.sql("DROP TABLE IF EXISTS AIRLINE")

    // create temporary staging table to load parquet data
    snSession.sql("CREATE EXTERNAL TABLE STAGING_AIRLINE " +
        "USING parquet OPTIONS(path " + s"'${dataFolder}/airlineParquetData')")

    pw.println("Create a column table AIRLINE")
    snSession.sql("CREATE TABLE AIRLINE USING column AS (SELECT Year AS Year_, " +
        "Month AS Month_ , DayOfMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, " +
        "CRSArrTime, UniqueCarrier, FlightNum, TailNum, ActualElapsedTime, " +
        "CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin, Dest, Distance, " +
        "TaxiIn, TaxiOut, Cancelled, CancellationCode, Diverted, CarrierDelay, " +
        "WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay, " +
        "ArrDelaySlot FROM STAGING_AIRLINE)")

    // sampling is used to run approaximate queries
    pw.println("Creating a sample table from AIRLINE table")

    // Attribute 'qcs' in the statement below specifies the columns used for
    // stratification and attribute 'fraction' specifies how big the sample needs to be
    // (3% of the base table AIRLINE in this case).
    // For full details of various option related to Synopsis Data Engine
    // refer to http://tibcosoftware.github.io/snappydata/aqp/
    snSession.sql("CREATE SAMPLE TABLE AIRLINE_SAMPLE ON AIRLINE OPTIONS" +
        "(qcs 'UniqueCarrier, Year_, Month_', fraction '0.03')  " +
        "AS (SELECT Year_, Month_ , DayOfMonth, " +
        "DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime, UniqueCarrier, " +
        "FlightNum, TailNum, ActualElapsedTime, CRSElapsedTime, AirTime, " +
        "ArrDelay, DepDelay, Origin, Dest, Distance, TaxiIn, TaxiOut, " +
        "Cancelled, CancellationCode, Diverted, CarrierDelay, WeatherDelay, " +
        "NASDelay, SecurityDelay, LateAircraftDelay, ArrDelaySlot FROM AIRLINE)")

    pw.println()
    pw.println("Creating a reference table AIRLINEREF")
    snSession.sql("DROP TABLE IF EXISTS STAGING_AIRLINEREF")
    snSession.sql("DROP TABLE IF EXISTS AIRLINEREF")

    // create temporary staging table to load parquet data
    snSession.sql("CREATE EXTERNAL TABLE STAGING_AIRLINEREF USING " +
        "parquet OPTIONS(path " + s"'${dataFolder}/airportcodeParquetData')")

    snSession.sql("CREATE TABLE AIRLINEREF USING row AS (SELECT CODE, " +
        "DESCRIPTION FROM STAGING_AIRLINEREF)")

    pw.println()
    pw.println("Executing approaximate queries")

    pw.println()
    pw.println("Which airline had the most flights each year?")
    // the 'with error 0.20' clause in the query below signals query engine to execute the
    // query on the sample table instead of the base table and maximum 20% error is allowed,
    // refer to http://tibcosoftware.github.io/snappydata/aqp/#running-queries for more details
    var result = snSession.sql("select  count(*) flightRecCount, description AirlineName, " +
        "UniqueCarrier carrierCode ,Year_ from airline , airlineref where " +
        "airline.UniqueCarrier = airlineref.code group by " +
        "UniqueCarrier,description, Year_ order by flightRecCount desc limit " +
        "10 with error 0.20").collect()
    pw.println("FlightRecCount, AirlineName, Carrier, Year")
    pw.println("-----------------------------------------------")
    result.foreach(r => pw.println(r(0) + ", " + r(1) + ", " + r(2) + ", " + r(3)))

    pw.println()
    pw.println("Which Airlines Arrive On Schedule?")
    // use 'with error' clause to instruct the query engine to run it on sample
    // table and return approaximate results
    result = snSession.sql("select AVG(ArrDelay) arrivalDelay, " +
        "relative_error(arrivalDelay) rel_err, UniqueCarrier " +
        "carrier from airline group by UniqueCarrier " +
        "order by arrivalDelay with error").collect()
    pw.println("ArrivalDelay, Relative_error, Carrier")
    pw.println("-------------------------------------")
    result.foreach(r => pw.println(r(0) + ", " + r(1) + ", " + r(2)))

    pw.println()
    pw.println("Which Airlines Arrive On Schedule? JOIN with reference table?")
    result = snSession.sql("select AVG(ArrDelay) arrivalDelay, " +
        "relative_error(arrivalDelay) rel_err, description AirlineName, " +
        "UniqueCarrier carrier from airline, airlineref " +
        "where airline.UniqueCarrier = airlineref.Code " +
        "group by UniqueCarrier, description order by arrivalDelay " +
        "with error").collect()
    pw.println("ArrivalDelay,  Relative_error,  AirlineName,  Carrier")
    pw.println("-----------------------------------------------------")
    result.foreach(r => pw.println(r(0) + ", " + r(1) + ", " + r(2) + ", " + r(3)))
  }

  def main(args: Array[String]): Unit = {
    parseArgs(args)

    val dataDirAbsolutePath = createAndGetDataDir

    println("Creating a SnappySession")
    val spark: SparkSession = SparkSession
        .builder
        .appName("SynopsisDataExample")
        .master("local[*]")
        // sys-disk-dir attribute specifies the directory where persistent data is saved
        .config("snappydata.store.sys-disk-dir", dataDirAbsolutePath)
        .config("snappydata.store.log-file", dataDirAbsolutePath + "/SnappyDataExample.log")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)

    val pw = new PrintWriter(System.out, true)
    runSynopsisDataExample(snSession, pw)
    pw.close()
  }

  def createAndGetDataDir: String = {
    // creating a directory to save all persistent data
    val dataDir = "./" + "snappydata_examples_data"
    new File(dataDir).mkdir()
    val dataDirAbsolutePath = new File(dataDir).getAbsolutePath
    dataDirAbsolutePath
  }

  private def parseArgs(args: Array[String]): Unit = {
    if (args.length != 1) {
      printUsage()
      System.exit(1)
    }
    dataFolder = args(0)
  }

  private def printUsage(): Unit = {
    val usage: String =
      "Usage: SynopsisDataExample <dataFolderPath> \n" +
          "\n" +
          "dataFolderPath - (string) local folder where data files airlineParquetData and airportcodeParquetData are located\n"
    println(usage)
  }
}
