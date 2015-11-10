package io.snappydata.app.twitter

import com.typesafe.config.ConfigFactory
import twitter4j._
import twitter4j.conf.{ConfigurationBuilder, Configuration}

/**
 * Created by ymahajan on 28/10/15.
 */
object TwitterStream {

  private val getTwitterConf: Configuration = {
    val twitterConf = new ConfigurationBuilder()
      .setOAuthConsumerKey("***REMOVED***")
      .setOAuthConsumerSecret("***REMOVED***")
      .setOAuthAccessToken("***REMOVED***")
      .setOAuthAccessTokenSecret("***REMOVED***")
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