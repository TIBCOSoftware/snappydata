/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package io.snappydata.streaming

import java.sql.{Connection, DriverManager}

import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper

class StreamingDUnitTest(val s: String) extends ClusterManagerTestBase(s) {

  override def tearDown2(): Unit = {
    super.tearDown2()
  }

  private def getANetConnection(netPort: Int): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    Class.forName(driver).newInstance //scalastyle:ignore
    val url = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  def testSnappyStreamingContextStartStop(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm1.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()
    s.execute("streaming stop")
    s.execute("streaming init 2secs")
    s.execute("streaming init 4secs")
    s.execute("create stream table tweetsTable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '0Xo8rg3W0SOiqu14HZYeyFPZi', " +
        "consumerSecret 'gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR', " +
        "accessToken '43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq', " +
        "accessTokenSecret 'aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu', " +
        "rowConverter 'io.snappydata.streaming.TweetToRowsConverter')")
    s.execute("streaming start")
    s.execute("streaming start")
    s.execute("streaming stop")
    s.execute("streaming stop")
    s.execute("drop table tweetsTable")
    conn.close()
  }
}
