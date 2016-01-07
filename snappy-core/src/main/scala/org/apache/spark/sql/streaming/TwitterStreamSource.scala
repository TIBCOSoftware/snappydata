package org.apache.spark.sql.streaming

import twitter4j.auth.{Authorization, OAuthAuthorization}
import twitter4j.conf.{Configuration, ConfigurationBuilder}

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils

final class TwitterStreamSource extends SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String],
      schema: StructType): BaseRelation = {
    new TwitterStreamRelation(sqlContext, options, schema)
  }
}

case class TwitterStreamRelation(@transient val sqlContext: SQLContext,
    options: Map[String, String],
    override val schema: StructType)
    extends StreamBaseRelation(options) {

  val consumerKey = options("consumerKey")
  val consumerSecret = options("consumerSecret")
  val accessToken = options("accessToken")
  val accessTokenSecret = options("accessTokenSecret")

  //  TODO Yogesh, need to pass this through DDL
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

  if (TwitterStreamRelation.getRowStream() == null) {
    rowStream = {
      TwitterUtils.createStream(context, Some(createOAuthAuthorization()),
        filters, storageLevel).flatMap(rowConverter.toRows)
    }
    TwitterStreamRelation.setRowStream(rowStream)
    // TODO Yogesh, this is required from snappy-shell, need to get rid of this
    rowStream.foreachRDD { rdd => rdd }
  } else {
    rowStream = TwitterStreamRelation.getRowStream()
  }
}

object TwitterStreamRelation extends Logging {
  private var rowStream: DStream[InternalRow] = null

  private val LOCK = new Object()

  private def setRowStream(stream: DStream[InternalRow]): Unit = {
    LOCK.synchronized {
      rowStream = stream
    }
  }

  private def getRowStream(): DStream[InternalRow] = {
    LOCK.synchronized {
      rowStream
    }
  }
}