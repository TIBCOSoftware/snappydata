package io.snappydata.app.twitter

import com.typesafe.config.ConfigFactory
import twitter4j._
import twitter4j.conf.{ConfigurationBuilder, Configuration}

/**
 * Created by ymahajan on 28/10/15.
 */
object TwitterStream {

  def main(args: Array[String]) {
    println("ok")
  }

  private val getTwitterConf: Configuration = {
    val twitterConf = new ConfigurationBuilder()
      .setOAuthConsumerKey("0Xo8rg3W0SOiqu14HZYeyFPZi")
      .setOAuthConsumerSecret("gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR")
      .setOAuthAccessToken("43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq")
      .setOAuthAccessTokenSecret("aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu")
      .setJSONStoreEnabled(true)
      .build()
    twitterConf
  }

  def getStream = new TwitterStreamFactory(getTwitterConf).getInstance()

  class OnTweetPosted(cb: Status => Unit) extends StatusListener {

    override def onStatus(status: Status): Unit = {
      cb(status)}
    override def onException(ex: Exception): Unit = throw ex

    // no-op for the following events
    override def onStallWarning(warning: StallWarning): Unit = {}
    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}
    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}
    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}
  }
}