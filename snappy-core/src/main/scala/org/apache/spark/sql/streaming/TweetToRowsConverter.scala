package org.apache.spark.sql.streaming

import org.json.{JSONArray, JSONObject}
import twitter4j.Status

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class TweetToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[Row] = {
    val status: Status = message.asInstanceOf[Status]
    Seq(Row.fromSeq(Seq(status.getId,
      status.getText,
      status.getUser().getName,
      status.getUser.getLang,
      status.getRetweetCount,
      status.getHashtagEntities.mkString(","))))
  }
}

class HashTagToRowsConverter extends StreamToRowsConverter with Serializable {
  override def toRows(message: Any): Seq[Row] = {
    val status: Status = message.asInstanceOf[Status]
    Seq(Row.fromSeq(Seq(status.getText)))
  }
}

class TweetToHashtagRow extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[Row] = {
    val schema = StructType(List(StructField("hashtag", StringType)))
    var json: JSONObject = null
    var arr: Array[Row] = null
    if (message.isInstanceOf[String]) {
      //for file stream
      json = new JSONObject(message.asInstanceOf[String])
      val hashArray = json.get("hashtagEntities").asInstanceOf[JSONArray]
      arr = new Array[Row](hashArray.length())
      for (i <- 0 until hashArray.length()) {
        val a = hashArray.getJSONObject(i)
        val b = a.getString("text")
        arr(i) = Row.fromSeq(Seq(b))
      }
    } else {
      //for twitter stream
      val status = message.asInstanceOf[Status]
      val hashArray = status.getHashtagEntities
      arr = new Array[Row](hashArray.length)
      for (i <- 0 until hashArray.length) {
        val b = hashArray(i).getText

        arr(i) = Row.fromSeq(Seq(b))
      }
    }

    arr.toSeq
  }
}


class TweetToRetweetRow extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[Row] = {
    var json: JSONObject = null
    var retweetCnt: Int = 0
    var retweetTxt: String = null
    var retweetId: Long = 0
    if (message.isInstanceOf[String]) {
      //for file stream
      json = new JSONObject(message.asInstanceOf[String])
      if (json != null && json.has("retweetedStatus")) {
        val retweetedSts = json.getJSONObject("retweetedStatus")
        retweetTxt = retweetedSts.getString("text")
        retweetCnt = retweetedSts.getInt("retweetCount")
        retweetId = retweetedSts.getLong("id")
      }
    } else {
      //for twitter stream
      val status = message.asInstanceOf[Status]
      if (status.getRetweetedStatus != null) {
        retweetTxt = status.getRetweetedStatus.getText
        retweetCnt = status.getRetweetedStatus.getRetweetCount
        retweetId = status.getRetweetedStatus.getId
      }
    }
    val sampleRow = new Array[Row](1)
    sampleRow(0) = Row.fromSeq(Seq(retweetId, retweetCnt, retweetTxt))
    sampleRow.toSeq
  }
}
