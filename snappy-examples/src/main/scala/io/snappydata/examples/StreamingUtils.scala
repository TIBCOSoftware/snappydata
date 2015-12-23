package io.snappydata.examples

import com.typesafe.config.Config
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.json.{JSONArray, JSONObject}
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.{ConfigurationBuilder}

object StreamingUtils {

  def convertTweetToRow(message: Any, schema: StructType): Seq[Row] = {
    var json : JSONObject = null
    var arr: Array[GenericRowWithSchema] = null
    if (message.isInstanceOf[String]){
      //for file stream
      json = new JSONObject(message.asInstanceOf[String])
      val id = json.get("id").asInstanceOf[Long]
      val txt = json.get("text").asInstanceOf[String]
      val reTweet = json.get("retweetCount").asInstanceOf[Int]
      val hashArray = json.get("hashtagEntities").asInstanceOf[JSONArray]
      arr = new Array[GenericRowWithSchema](hashArray.length())
      for (i <- 0 until hashArray.length()) {
        val a = hashArray.getJSONObject(i)
        val b = a.getString("text")

        arr(i) = new GenericRowWithSchema(Array(id, UTF8String.fromString(txt),
          reTweet, UTF8String.fromString(b)), schema)

      }
    }else {
      //for twitter stream
      val status = message.asInstanceOf[Status]
      val hashArray = status.getHashtagEntities
      arr = new Array[GenericRowWithSchema](hashArray.length)
      for (i <- 0 until hashArray.length) {
        val a = hashArray(i).getText

        arr(i) = new GenericRowWithSchema(Array(status.getId, UTF8String.fromString(status.getText),
          status.getRetweetCount, UTF8String.fromString(a)), schema)

      }
    }

    arr.toSeq

  }

  def getTwitterAuth(jobConfig: Config): OAuthAuthorization = {
    // Generate twitter configuration and authorization
    new OAuthAuthorization(
      new ConfigurationBuilder()
        .setOAuthConsumerKey(jobConfig.getString("consumerKey"))
        .setOAuthConsumerSecret(jobConfig.getString("consumerSecret"))
        .setOAuthAccessToken(jobConfig.getString("accessToken"))
        .setOAuthAccessTokenSecret(jobConfig.getString("accessTokenSecret"))
        .setJSONStoreEnabled(true)
        .build())

  }
}

