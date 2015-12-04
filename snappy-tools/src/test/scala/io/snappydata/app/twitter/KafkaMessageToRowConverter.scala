package io.snappydata.app.twitter

/**
 * Created by ymahajan on 5/11/15.
 */

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.streaming.MessageToRowConverter
import org.apache.spark.unsafe.types.UTF8String
import twitter4j.{Status, TwitterObjectFactory}

import scala.util.Random

class KafkaMessageToRowConverter extends MessageToRowConverter with Serializable {

  override def toRow(message: Any): InternalRow = {
    val status: Status = TwitterObjectFactory.createStatus(message.asInstanceOf[String])
    InternalRow.fromSeq(Seq(status.getId, UTF8String.fromString(status.getText),
      UTF8String.fromString(status.getUser().getName), UTF8String.fromString(status.getUser.getLang),
      status.getRetweetCount,UTF8String.fromString(status.getHashtagEntities.mkString(",")) ))
  }

  /*override def toRow(message: Any): Seq[InternalRow] = {
    //TODO Yogesh. convert this raw JSON string to twitter4j.SatusJSONImpl
    val status : Status = TwitterObjectFactory.createStatus(message.asInstanceOf[String])
    //val tweet = new JSONObject(message.asInstanceOf[String])
    val hashTags = status.getHashtagEntities

    val limit = KafkaMessageToRowConverter.rand.nextInt(20000)
    (0 until limit).flatMap { i =>
      val id = status.getId + (i.toLong *
        KafkaMessageToRowConverter.rand.nextInt(100000))
      if (hashTags.length <= 1) {
        Seq(InternalRow.fromSeq(Seq(id,
          UTF8String.fromString(status.getText),
          UTF8String.fromString(status.getUser().getName),
          UTF8String.fromString(status.getUser.getLang),
          status.getRetweetCount,
          UTF8String.fromString(if (hashTags.isEmpty) "" else hashTags(0).getText))))
      } else {
        hashTags.map { tag =>
          InternalRow.fromSeq(Seq(id,
            UTF8String.fromString(status.getText),
            UTF8String.fromString(status.getUser().getName),
            UTF8String.fromString(status.getUser.getLang),
            status.getRetweetCount, UTF8String.fromString(tag.getText)))
        }
      }
    }
    //Row.fromSeq
  }*/
  override def getTargetType = classOf[String]
}

object KafkaMessageToRowConverter {
  private val rand = new Random
}