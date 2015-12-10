package io.snappydata.app.streaming

import io.snappydata.SnappyFunSuite
import org.apache.spark.{SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.StreamingSnappyContext
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.streaming.{Seconds, Duration, StreamingContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter}


/**
 * Created by ymahajan on 26/10/15.
 */
class StreamSamplingSuite extends SnappyFunSuite with Eventually with BeforeAndAfter {

  private var ssc: StreamingContext = _

  private var ssnc: StreamingSnappyContext = _

  def framework: String = this.getClass.getSimpleName

  def master: String = "local[2]"

  def batchDuration: Duration = Seconds(1)

/*  override def newSparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(framework)
    sparkConf
  }*/

  override def afterAll(): Unit = {
    if (ssnc != null) {
      StreamingSnappyContext.stop()
    }
    super.afterAll()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    ssc = new StreamingContext(sc, batchDuration)
    ssnc = StreamingSnappyContext(ssc)
  }


  val urlString = "jdbc:snappydata:;locators=localhost:10101;persist-dd=false;member-timeout=600000;" +
    "jmx-manager-start=false;enable-time-statistics=false;statistic-sampling-enabled=false"

  val props = Map(
    "url" -> urlString,
    "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
    "poolImpl" -> "tomcat",
    "poolProperties" -> "maxActive=300",
    "user" -> "app",
    "password" -> "app"
  )


  test("sql stream sampling") {

    ssnc.sql("create stream table tweetstreamtable (id long, text string, fullName string, " +
      "country string, retweets int, hashtag string) " +
      "using twitter_stream options (" +
      "consumerKey '0Xo8rg3W0SOiqu14HZYeyFPZi', " +
      "consumerSecret 'gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR', " +
      "accessToken '43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq', " +
      "accessTokenSecret 'aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu', " +
      "streamToRow 'io.snappydata.app.streaming.TweetToRowConverter')")

    val tableStream = ssnc.getSchemaDStream("tweetstreamtable")

    ssnc.registerSampleTable("tweetstreamtable_sampled", tableStream.schema, Map(
      "qcs" -> "hashtag",
      "fraction" -> "0.05",
      "strataReservoirSize" -> "300",
      "timeInterval" -> "3m"), Some("tweetstreamtable"))

    ssnc.saveStream(tableStream, Seq("tweetstreamtable_sampled"), {
      (rdd: RDD[Row], _) => rdd
    }, tableStream.schema)

    ssnc.sql("create table rawStreamColumnTable(id long, " +
      "text string, " +
      "fullName string, " +
      "country string, " +
      "retweets int, " +
      "hashtag string) " +
      "using column " +
      "options('PARTITION_BY','id')")

    var numTimes = 0
    tableStream.foreachRDD { rdd =>
      //var start: Long = 0
      //var end: Long = 0
      val df = ssnc.createDataFrame(rdd, tableStream.schema)
      df.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("rawStreamColumnTable")

      println("Top 10 hash tags from exact table")
      //start = System.nanoTime()

      val top10Tags = ssnc.sql("select count(*) as cnt, hashtag from rawStreamColumnTable " +
        "where length(hashtag) > 0 group by hashtag order by cnt desc limit 10").collect()
      //end = System.nanoTime()
      top10Tags.foreach(println)
      //println("\n\nTime taken: " + ((end - start) / 1000000L) + "ms")

      numTimes += 1
      if ((numTimes % 18) == 1) {
        ssnc.sql("SELECT count(*) FROM rawStreamColumnTable").show()
      }

      println("Top 10 hash tags from sample table")
      //start = System.nanoTime()
      val stop10Tags = ssnc.sql("select count(*) as cnt, hashtag from tweetstreamtable_sampled " +
        "where length(hashtag) > 0 group by hashtag order by cnt desc limit 10").collect()
      //end = System.nanoTime()
      stop10Tags.foreach(println)
      //println("\n\nTime taken: " + ((end - start) / 1000000L) + "ms")

    }

    ssnc.sql( """STREAMING CONTEXT START """)
    ssc.awaitTerminationOrTimeout(30 * 1000)
  }
}
