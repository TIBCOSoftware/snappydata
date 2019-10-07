package io.snappydata.filodb

import java.sql.{DriverManager, PreparedStatement}
import java.util.concurrent.Executors

import scala.concurrent.duration.Duration
import scala.util.{Failure, Random, Success}

import org.apache.spark.sql.{SaveMode, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This application depicts how a Spark cluster can
 * connect to a Snappy cluster to fetch and query the tables
 * using Scala APIs in a Spark App.
 */
object FiloDBApp_Row {

  def main(args: Array[String]) {
    // scalastyle:off println

    val taxiCsvFile = args(0)
    val numRuns = 50

    // Queries
    val medallions = Array("23A89BC906FBB8BD110677FBB0B0A6C5",
      "3F8D8853F7EF89A7630565DDA38E9526",
      "3FE07421C46041F4801C7711F5971C63",
      "789B8DC7F3CB06A645B0CDC24591B832",
      "18E80475A4E491022BC2EF8559DABFD8",
      "761033F2C6F96EBFA9F578E968FDEDE5",
      "E4C72E0EE95C31D6B1FEFCF3F876EF90",
      "AF1421FCAA4AE912BDFC996F8A9B5675",
      "FB085B55ABF581ADBAD3E16283C78C01",
      "29CBE2B638D6C9B7239D2CA7A72A70E9")

    // trip info for a single driver within a given time range
    val singleDriverQueries = (1 to 20).map { i =>
      val medallion = medallions(Random.nextInt(medallions.size))
      //      s"SELECT avg(trip_distance), avg(passenger_count) from nyctaxi where medallion = '$medallion'" +
      //          s" AND pickup_datetime > '2013-01-15T00Z' AND pickup_datetime < '2013-01-22T00Z'"
      s"SELECT AVG(TRIP_DISTANCE), AVG(PASSENGER_COUNT) FROM NYCTAXI WHERE MEDALLION = '$medallion'" +
          s" AND PICKUP_DATETIME > '2013-01-15T00Z' AND PICKUP_DATETIME < '2013-01-22T00Z'"
    }

    // average trip distance by day for several days

    val allQueries = singleDriverQueries


    val props = Map(
      "poolImpl" -> "tomcat",
      "poolProperties" -> "maxActive=256"
    )

    val conf = (new SparkConf).setMaster("local[8]")
        .setAppName("test")
        .set("spark.scheduler.mode", "FAIR")
        .set("spark.ui.enabled", "false")
        .set(io.snappydata.Constant.STORE_PROPERTY_PREFIX + "conserve-sockets", "false")

    val sc = new SparkContext(conf)
    val snc = SnappyContext(sc)
    snc.sql("set spark.sql.shuffle.partitions=4")
    snc.dropTable("NYCTAXI", ifExists = true)

    // Ingest file - note, this will take several minutes
    puts("Starting ingestion...")
    val csvDF = snc.read.format("com.databricks.spark.csv").
        option("header", "true").option("inferSchema", "false").load(taxiCsvFile)
    puts(s"csvDF count : ${csvDF.count()}")


    val usingOptionString =
      s"""
           USING row
           OPTIONS ()"""

    val usingOptionString1 = s" USING row OPTIONS (PARTITION_BY 'MEDALLION', BUCKETS '8')"


    snc.sql(
      s"""CREATE TABLE NYCTAXI (MEDALLION VARCHAR(100) NOT NULL,
         			HACK_LICENSE VARCHAR(100),
         			VENDOR_ID VARCHAR(100),
         			RATE_CODE INTEGER,
         			STORE_AND_FWD_FLAG VARCHAR(100),
         			PICKUP_DATETIME VARCHAR(100),
         			DROPOFF_DATETIME VARCHAR(100),
         			PASSENGER_COUNT INTEGER,
         			TRIP_TIME_IN_SECS INTEGER,
         			TRIP_DISTANCE DOUBLE,
         			PICKUP_LONGITUDE DOUBLE,
         			PICKUP_LATITUDE DOUBLE,
         			DROPOFF_LONGITUDE DOUBLE,
         			DROPOFF_LATITUDE DOUBLE
         			)
        """ + usingOptionString1
    )

    puts("Table Created")

    snc.sql(
      "CREATE INDEX INDEX_PICKUP_DATETIME ON NYCTAXI (MEDALLION)"
    )
    puts("Index Created")

    csvDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("NYCTAXI")
    //csvDF.insertInto("NYCTAXI")
    puts("Ingestion done.")

    Thread sleep 2000


    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent._

    implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))

    val cachedPS : collection.mutable.HashMap[String, PreparedStatement] = new collection.mutable.HashMap[String, PreparedStatement]

    def prepareForQuery(query: String) : PreparedStatement = {
      val connection = DriverManager.getConnection("jdbc:snappydata:")
      connection.setAutoCommit(false)
      connection.prepareStatement(query)
    }

    def getFuturePrepStatement(query: String): Future[PreparedStatement] = {
      val task: Future[PreparedStatement] = Future {
        cachedPS.getOrElseUpdate(query, prepareForQuery(query))
      }
      task.onComplete {
        {
          case Success(value) => {
            value.synchronized {
              //synchronized(value) {
              val rs = value.executeQuery()
              while (rs.next()) {
              }
            }
          }
          case Failure(e) => println(s"D'oh! The Future failed: ${e.getMessage}")
        }
      }
      task
    }

    def runQueries(queries: Array[String], numQueries: Int = 1000): Unit = {
      val startMillis = System.currentTimeMillis
      val futures = (0 until numQueries).map(i => getFuturePrepStatement(queries(Random.nextInt(queries.size))))
      val f = Future.sequence(futures.toList)
      Await.ready(f, Duration.Inf)
      val endMillis = System.currentTimeMillis
      val qps = numQueries / ((endMillis - startMillis) / 1000.0)
      puts(s"Ran $numQueries queries in ${endMillis - startMillis} millis.  QPS = $qps")
    }

    puts("Warming up...")
    runQueries(allQueries.toArray, 100)
    Thread sleep 2000
    puts("Now running queries for real...")
    (0 until numRuns).foreach { i => runQueries(allQueries.toArray) }

    sc.stop()

  }

  def puts(s: String): Unit = {
    //scalastyle:off
    println(s)
    //scalastyle:on
  }
}
