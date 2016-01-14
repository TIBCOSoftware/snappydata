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
      val hashArray = json.get("hashtagEntities").asInstanceOf[JSONArray]
      arr = new Array[GenericRowWithSchema](hashArray.length())
      for (i <- 0 until hashArray.length()) {
        val a = hashArray.getJSONObject(i)
        val b = a.getString("text")

        arr(i) = new GenericRowWithSchema(Array(UTF8String.fromString(b)), schema)
      }
    }else {
      //for twitter stream
      val status = message.asInstanceOf[Status]
      val hashArray = status.getHashtagEntities
      arr = new Array[GenericRowWithSchema](hashArray.length)
      for (i <- 0 until hashArray.length) {
        val b = hashArray(i).getText

        arr(i) = new GenericRowWithSchema(Array(UTF8String.fromString(b)), schema)
      }
    }

    arr.toSeq

  }

  def convertPopularTweetsToRow(message: Any):  Array[TwitterSchema] = {
    var json: JSONObject = null
    var retweetCnt : Int = 0
    var retweetTxt : String = null
    if (message.isInstanceOf[String]) {
      //for file stream
      json = new JSONObject(message.asInstanceOf[String])
      if(json != null && json.has("retweetedStatus")) {
        val retweetedSts = json.getJSONObject("retweetedStatus")
        retweetTxt = retweetedSts.get("text").asInstanceOf[String]
        retweetCnt = retweetedSts.get("retweetCount").asInstanceOf[Int]
      }
    } else {
      //for twitter stream
      val status = message.asInstanceOf[Status]
      if(status.getRetweetedStatus != null) {
        retweetTxt = status.getRetweetedStatus.getText
        retweetCnt = status.getRetweetedStatus.getRetweetCount
      }
    }
    val sampleRow = new Array[TwitterSchema](1)
    sampleRow(0) = new TwitterSchema(retweetCnt, retweetTxt)
    sampleRow
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

case class TwitterSchema(retweetCnt : Int, retweetTxt: String)
