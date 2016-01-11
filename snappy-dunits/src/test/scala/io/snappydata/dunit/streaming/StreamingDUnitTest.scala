package io.snappydata.dunit.streaming

import java.sql.{Connection, DriverManager}

import dunit.AvailablePortHelper
import io.snappydata.dunit.cluster.ClusterManagerTestBase

/**
  * Created by ymahajan on 23/12/15.
  */
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
  def testStreamingSQL(): Unit = {

  }
  def _testStreamingSQL(): Unit = {
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
    s.execute("streaming start")
    for (a <- 1 to 5) {
      Thread.sleep(2000)
      s.execute("select text, fullName from tweetsTable where text like '%e%'")
      val rs = s.getResultSet
      while (rs.next()) {
        println("YOGS RESULTSET: " + rs.getString(1) + ", " + rs.getString(2))
      }
    }
    s.execute("drop table tweetsTable")
    s.execute("streaming stop")
    conn.close()
  }
}

