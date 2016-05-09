package io.snappydata.streaming

import twitter4j.Status

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.StreamToRowsConverter

class TweetToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[Row] = {
    val status: Status = message.asInstanceOf[Status]
    Seq(Row.fromSeq(Seq(status.getId,
      status.getText,
      status.getUser().getName,
      status.getUser.getLang,
      status.getRetweetCount,
      status.getHashtagEntities.map(_.getText).mkString(","))))
  }
}
