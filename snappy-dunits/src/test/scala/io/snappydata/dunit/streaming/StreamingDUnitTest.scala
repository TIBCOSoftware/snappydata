package io.snappydata.dunit.streaming

import java.sql.{Connection, DriverManager}

import dunit.AvailablePortHelper
import io.snappydata.dunit.cluster.ClusterManagerTestBase

import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.streaming.SnappyStreamingContext

class StreamingDUnitTest(val s: String) extends ClusterManagerTestBase(s) {

  override def tearDown2(): Unit = {
    super.tearDown2()
  }

  private def getANetConnection(netPort: Int): Connection = {
    val driver = "com.pivotal.gemfirexd.jdbc.ClientDriver"
    Class.forName(driver).newInstance //scalastyle:ignore
    val url = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  def testStreamingAdhocSQL(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm1.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()
    s.execute("streaming init 2")
    s.execute("create stream table tweetsTable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '0Xo8rg3W0SOiqu14HZYeyFPZi', " +
        "consumerSecret 'gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR', " +
        "accessToken '43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq', " +
        "accessTokenSecret 'aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu', " +
        "rowConverter 'io.snappydata.dunit.streaming.TweetToRowsConverter')")
    s.execute("create table fullTweetsTable (id long, text string, " +
        "fullName string, country string, retweets int, " +
        "hashtag string) using column")
    s.execute("create topk table topkTweets on tweetsTable options(" +
        s"key 'hashtag', " +
        "timeInterval '10000ms', size '10')")

    val tweetsStream = SnappyStreamingContext.getActive.get
        .getSchemaDStream("tweetsTable")
    tweetsStream.foreachDataFrame { df =>
      // println("SW: inserting into fullTweetsTable rows: " + df.collect().mkString("\n"))
      df.write.insertInto("fullTweetsTable")
    }

    val snc = SnappyContext()

    s.execute("streaming start")

    for (a <- 1 to 5) {

      Thread.sleep(2000)

      s.execute("select hashtag, count(hashtag) as cht from fullTweetsTable " +
          "group by hashtag order by cht desc limit 10")
      println("\n\n-----  TOP Tweets  -----\n")
      var rs = s.getResultSet
      if (a == 5) assert(rs.next)
      while (rs.next()) {
        println(s"${rs.getString(1)} ; ${rs.getInt(2)}")
      }

      s.execute("select * from topkTweets order by EstimatedValue desc")
      println("\n\n-----  TOPK Tweets  -----\n")
      rs = s.getResultSet
      var numResults = 0
      while (rs.next()) {
        println(s"${rs.getString(1)} ; ${rs.getLong(2)} ; ${rs.getString(3)}")
        numResults += 1
      }
      println(s"Num results=$numResults")
      assert(numResults <= 10)

      val topKdf = snc.queryApproxTSTopK("topkTweets", -1, -1)
      println("\n\n-----  TOPK Tweets2  -----\n")
      val topKRes = topKdf.collect()
      topKRes.foreach(println)
      numResults = topKRes.length
      println(s"Num results=$numResults")
      assert(numResults <= 10)

      s.execute("select text, fullName from fullTweetsTable " +
          "where text like '%e%'")
      rs = s.getResultSet
      if (a == 5) assert(rs.next)
      while (rs.next()) {
        rs.getString(1)
        rs.getString(2)
      }
    }
    s.execute("streaming stop")
    s.execute("drop table fullTweetsTable")
    s.execute("drop table topktweets")
    s.execute("drop table tweetsTable")

    conn.close()
  }

  def testSnappyStreamingContextStartStop(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm1.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()
    s.execute("streaming stop")
    s.execute("streaming init 2")
    s.execute("streaming init 4")
    s.execute("create stream table tweetsTable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '0Xo8rg3W0SOiqu14HZYeyFPZi', " +
        "consumerSecret 'gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR', " +
        "accessToken '43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq', " +
        "accessTokenSecret 'aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu', " +
        "rowConverter 'io.snappydata.dunit.streaming.TweetToRowsConverter')")
    s.execute("streaming start")
    s.execute("streaming start")
    s.execute("streaming stop")
    s.execute("streaming stop")
    s.execute("drop table tweetsTable")
    conn.close()
  }
}
