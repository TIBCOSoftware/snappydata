package org.apache.spark.sql.streaming

import org.json.{JSONArray, JSONObject}
import twitter4j.Status

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class TweetToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[InternalRow] = {
    val status: Status = message.asInstanceOf[Status]
    Seq(InternalRow.fromSeq(Seq(status.getId,
      UTF8String.fromString(status.getText),
      UTF8String.fromString(status.getUser().getName),
      UTF8String.fromString(status.getUser.getLang),
      status.getRetweetCount, UTF8String.fromString(
        status.getHashtagEntities.mkString(",")))))
  }

}

class HashTagToRowsConverter extends StreamToRowsConverter with Serializable {
  override def toRows(message: Any): Seq[InternalRow] = {
    val status: Status = message.asInstanceOf[Status]
    Seq(InternalRow.fromSeq(Seq(UTF8String.fromString(status.getText))))
  }
}

class TweetToHashtagRow extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[InternalRow] = {
    val schema = StructType(List(StructField("hashtag", StringType)))
    var json: JSONObject = null
    var arr: Array[InternalRow] = null
    if (message.isInstanceOf[String]) {
      //for file stream
      json = new JSONObject(message.asInstanceOf[String])
      val hashArray = json.get("hashtagEntities").asInstanceOf[JSONArray]
      arr = new Array[InternalRow](hashArray.length())
      for (i <- 0 until hashArray.length()) {
        val a = hashArray.getJSONObject(i)
        val b = a.getString("text")
        arr(i) = InternalRow.fromSeq(Seq(UTF8String.fromString(b)))
      }
    } else {
      //for twitter stream
      val status = message.asInstanceOf[Status]
      val hashArray = status.getHashtagEntities
      arr = new Array[InternalRow](hashArray.length)
      for (i <- 0 until hashArray.length) {
        val b = hashArray(i).getText

        arr(i) = InternalRow.fromSeq(Seq(UTF8String.fromString(b)))
      }
    }

    arr.toSeq
  }
}

class TweetToRetweetRow extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[InternalRow] = {
    var json: JSONObject = null
    var retweetCnt: Int = 0
    var retweetTxt: String = null
    if (message.isInstanceOf[String]) {
      //for file stream
      json = new JSONObject(message.asInstanceOf[String])
      if (json != null && json.has("retweetedStatus")) {
        val retweetedSts = json.getJSONObject("retweetedStatus")
        retweetTxt = retweetedSts.get("text").asInstanceOf[String]
        retweetCnt = retweetedSts.get("retweetCount").asInstanceOf[Int]
      }
    } else {
      //for twitter stream
      val status = message.asInstanceOf[Status]
      if (status.getRetweetedStatus != null) {
        retweetTxt = status.getRetweetedStatus.getText
        retweetCnt = status.getRetweetedStatus.getRetweetCount
      }
    }
    val sampleRow = new Array[InternalRow](1)
    sampleRow(0) = InternalRow.fromSeq(Seq(retweetCnt, UTF8String.fromString(retweetTxt)))
    sampleRow.toSeq
  }
}
