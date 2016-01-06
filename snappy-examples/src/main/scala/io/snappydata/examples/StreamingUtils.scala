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
    var retweetTxt : String = null
    var retweetCnt : Int = 0
    if (message.isInstanceOf[String]){
      //for file stream
      json = new JSONObject(message.asInstanceOf[String])
      val id = json.get("id").asInstanceOf[Long]
      val txt = json.get("text").asInstanceOf[String]
      if(json != null && json.has("retweetedStatus")) {
        val retweetedSts = json.getJSONObject("retweetedStatus")
        retweetTxt = retweetedSts.get("text").asInstanceOf[String]
        retweetCnt = retweetedSts.get("retweetCount").asInstanceOf[Int]
      }
      val hashArray = json.get("hashtagEntities").asInstanceOf[JSONArray]
      arr = new Array[GenericRowWithSchema](hashArray.length())
      for (i <- 0 until hashArray.length()) {
        val a = hashArray.getJSONObject(i)
        val b = a.getString("text")
        arr(i) = new GenericRowWithSchema(Array(id, UTF8String.fromString(txt),
          UTF8String.fromString(b),retweetCnt,UTF8String.fromString(retweetTxt)), schema)

      }
    }else {
      //for twitter stream
      val status = message.asInstanceOf[Status]
      if(status.getRetweetedStatus != null) {
        retweetTxt = status.getRetweetedStatus.getText()
        retweetCnt = status.getRetweetedStatus.getRetweetCount()
      }
      val hashArray = status.getHashtagEntities
      arr = new Array[GenericRowWithSchema](hashArray.length)
      for (i <- 0 until hashArray.length) {
        val a = hashArray(i).getText
        arr(i) = new GenericRowWithSchema(Array(status.getId, UTF8String.fromString(status.getText),
          UTF8String.fromString(a), retweetCnt,
          UTF8String.fromString(retweetTxt)), schema)

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

