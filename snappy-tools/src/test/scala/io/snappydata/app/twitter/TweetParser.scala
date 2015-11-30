package io.snappydata.app.twitter

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.streaming.MessageToRowConverter
import org.apache.spark.unsafe.types.UTF8String
import org.codehaus.jackson.JsonToken._
import org.codehaus.jackson._
import twitter4j.TwitterObjectFactory
import twitter4j.Status
/**
 * Created by ymahajan on 5/11/15.
 */

class KafkaMessageToRowConverter extends MessageToRowConverter with Serializable {
  override def toRow(message: Any): InternalRow = {
    //val parser = new TweetParser()
    //val tweet: ParsedTweet = parser.parse(message).get
    //InternalRow.fromSeq(Seq(tweet.id, UTF8String.fromString(tweet.text), UTF8String.fromString(tweet.fullname), UTF8String.fromString(tweet.country)))
    //TODO Yogesh. convert this raw JSON string to twitter4j.SatusJSONImpl
    val status : Status = TwitterObjectFactory.createStatus(message.asInstanceOf[String])

    //Row.fromSeq
    InternalRow.fromSeq(Seq(status.getId, UTF8String.fromString(status.getText),
      UTF8String.fromString(status.getUser().getName), UTF8String.fromString(status.getUser.getLang),
      status.getRetweetCount))
  }
  override def getTargetType = classOf[String]
}

case class ParsedTweet(id: Long, text: String, fullname: String, country: String)

class TweetParser {
  val factory = new JsonFactory()

  def parse(str: String) = {
    val parser = factory.createJsonParser(str)
    var nested = 0
    if (parser.nextToken() == START_OBJECT) {
      var token = parser.nextToken()
      var idOpt: Option[Long] = None
      var textOpt: Option[String] = None
      var countryOpt: Option[String] = None
      var fullnameOpt: Option[String] = None

      while (token != null) {
        if (token == FIELD_NAME && nested == 0) {
          parser.getCurrentName() match {
            case "id" => {
              parser.nextToken()
              idOpt = Some(parser.getLongValue())
            }
            case "text" => {
              parser.nextToken()
              textOpt = Some(parser.getText())
            }
            case "country" => {
              parser.nextToken()
              countryOpt = Some(parser.getText())
            }
            case "full_name" => {
              parser.nextToken()
              fullnameOpt = Some(parser.getText())
            }
            case _ => //or else case
          }
        } else if (token == START_OBJECT) {
          nested += 1
        } else if (token == END_OBJECT) {
          nested -= 1
        }
        token = parser.nextToken()
      }
        Some(ParsedTweet(idOpt.get, textOpt.get, fullnameOpt.get, countryOpt.get))
    }
    else {
      None
    }
  }
}