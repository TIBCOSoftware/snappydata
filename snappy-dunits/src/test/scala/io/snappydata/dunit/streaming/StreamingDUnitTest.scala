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
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()
    s.execute("create stream table tweetsTable " +
      "(id long, text string, fullName string, " +
      "country string, retweets int, hashtag string) " +
      "using twitter_stream options (" +
      "consumerKey '***REMOVED***', " +
      "consumerSecret '***REMOVED***', " +
      "accessToken '***REMOVED***', " +
      "accessTokenSecret '***REMOVED***', " +
      "streamToRows 'io.snappydata.app.streaming.TweetToRowsConverter')")
    s.getResultSet
    s.execute("select * from tweetsTable")
    s.getResultSet
    conn.close()
  }
}

