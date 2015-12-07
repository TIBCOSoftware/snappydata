package org.apache.spark.sql.streaming


import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{DeletableRelation, DestroyRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.util.Utils
import twitter4j.auth.{OAuthAuthorization, NullAuthorization, Authorization}
import twitter4j.conf.{ConfigurationBuilder, Configuration}

/**
 * Created by ymahajan on 4/12/15.
 */
case class TwitterStreamRelation(@transient val sqlContext: SQLContext,
                                 options: Map[String, String],
                                 override val schema: StructType)
  extends StreamBaseRelation with Logging with StreamPlan with Serializable {

  @transient val context = StreamingCtxtHolder.streamingContext

  val storageLevel = options.get("storageLevel")
    .map(StorageLevel.fromString)
    .getOrElse(StorageLevel.MEMORY_AND_DISK_SER_2)

  val consumerKey = options("consumerKey")
  val consumerSecret = options("consumerSecret")
  val accessToken = options("accessToken")
  val accessTokenSecret = options("accessTokenSecret")

  //TODO Yogesh, need to pass this through DDL
  val filters = Seq("e")

  private val getTwitterConf: Configuration = {
    val twitterConf = new ConfigurationBuilder()
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
      .setJSONStoreEnabled(true)
      .build()
    twitterConf
  }

  private def createOAuthAuthorization(): Authorization = {
    new OAuthAuthorization(getTwitterConf)
  }

  @transient val twitterStream = {
    TwitterUtils.createStream(context, Some(createOAuthAuthorization()),
      filters, storageLevel)
  }

  private val streamToRow = {
    try {
      val clz = Utils.getContextOrSparkClassLoader.loadClass(options("streamToRow"))
      clz.newInstance().asInstanceOf[MessageToRowConverter]
    } catch {
      case e: Exception => sys.error(s"Failed to load class : ${e.toString}")
    }
  }

  @transient val stream: DStream[InternalRow] = twitterStream.map(streamToRow.toRow)
}

