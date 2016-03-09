// scalastyle:ignore
package io.snappydata.aggr

object Constants {
  val NumPublishers = 5
  val NumAdvertisers = 3

  val Publishers = (0 to NumPublishers).map("publisher_" +)
  val Advertisers = (0 to NumAdvertisers).map("advertiser_" +)
  val UnknownGeo = "unknown"
  val Geos = Seq("NY", "CA", "FL", "MI", "HI", UnknownGeo)
  val NumWebsites = 10000
  val NumCookies = 10000

  val KafkaTopic = "adnetwork-topic"
}