package org.apache.spark.sql.streaming

import twitter4j.auth.{Authorization, OAuthAuthorization}
import twitter4j.conf.{Configuration, ConfigurationBuilder}

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.twitter.TwitterUtils

/**
  * Created by ymahajan on 4/12/15.
  */
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

  @transient val twitterStream = {
    TwitterUtils.createStream(context, Some(createOAuthAuthorization()),
      filters, storageLevel)
  }

  stream = twitterStream.flatMap(rowConverter.toRows)
  // TODO Yogesh, this is required from snappy-shell, need to get rid of this
  stream.foreachRDD(rdd => {
    rdd
  })
}