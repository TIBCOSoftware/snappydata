package io.snappydata.app.twitter

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{SchemaDStream, StreamingSnappyContext}
import org.apache.spark.streaming._

/**
 * Created by ymahajan on 28/10/15.
 */
object KafkaConsumer {

  val props = Map(
    "url" -> "jdbc:gemfirexd:;mcast-port=33619;user=app;password=app;persist-dd=false",
    "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
    "poolImpl" -> "tomcat",
    "user" -> "app",
    "password" -> "app"
  )
  val options =  "OPTIONS (url 'jdbc:gemfirexd:;mcast-port=33619;user=app;password=app;persist-dd=false' ," +
    "driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver' ," +
    "poolimpl 'tomcat', " +
    "user 'app', " +
    "password 'app' ) "


  def main(args: Array[String]) {
    val sc = new org.apache.spark.SparkConf()
      .setMaster("local[2]")
      .setAppName("kafkaconsumer")
    val ssc = new StreamingContext(new SparkContext(sc), Milliseconds(200))
    val snsc = StreamingSnappyContext(ssc);

    val streamTable = "directKafkaStreamTable"
    snsc.sql("create stream table "+ streamTable + " (id long, text string, fullName string, country string, retweets int) using kafka options (storagelevel 'MEMORY_AND_DISK_SER_2', streamToRow 'io.snappydata.app.twitter.KafkaMessageToRowConverter' ," +
      " kafkaParams 'metadata.broker.list->localhost:9092', topics 'tweets')")

    snsc.udf.register("sentiment", sentiment _)

    //snsc.sql("select * from directKafkaStreamTable").show()

    //val resultSet: SchemaDStream = snsc.registerCQ("SELECT * FROM directKafkaStreamTable window (duration '2' seconds, slide '2' seconds) where country='jp'")
    //val resultSet: SchemaDStream = snsc.registerCQ("SELECT * FROM directKafkaStreamTable window (duration '2' seconds, slide '2' seconds) where text like '%girl%' limit 10")

   // val resultSet: SchemaDStream = snsc.registerCQ("SELECT retweets, max(retweets), min(retweets), avg(retweets) FROM directKafkaStreamTable window (duration '20' seconds, slide '20' seconds) group by retweets")
    val resultSet: SchemaDStream = snsc.registerCQ("select sentiment(text) from directKafkaStreamTable window (duration '10' seconds, slide '10' seconds) where text like '%girl%' group by sentiment(text)")
    //val resultSet: SchemaDStream = snsc.registerCQ("SELECT retweets, max(retweets), min(retweets), avg(retweets) FROM directKafkaStreamTable window (duration '20' seconds, slide '20' seconds) group by retweets")

    resultSet.foreachRDD(rdd => println(s"Received Twitter stream results. Count: ${if(!rdd.isEmpty()) rdd.first()}"))
    //resultSet.foreachRDD { r => r.foreach(println) }


//    snsc.cacheTable(streamTable)


//    snsc.sql("create sampled table directKafkaStreamTable_sampled Options (basetable 'directKafkaStreamTable', qcs 'text', fraction '0.005', strataReservoirSize '100' )")
//    snsc.cacheTable("directKafkaStreamTable_sampled")
//    snsc.sql("select count(*) as count from directKafkaStreamTable_sampled").show()

//    val directKafkaStoreTable = "directKafkaStoreTable"
//    snsc.sql("CREATE TABLE " + directKafkaStoreTable + " USING column " +
//      options + " AS (SELECT * FROM " + streamTable + ")")
//    snsc.sql("SELECT * FROM " + directKafkaStoreTable).show()
//    snsc.sql("DROP TABLE " + directKafkaStoreTable)

//    val tableDStream: SchemaDStream = snsc.getSchemaDStream("directKafkaStreamTable")
//    import org.apache.spark.sql.streaming.snappy._
//    tableDStream.saveToExternalTable("kafkaStreamGemXdTable", tableDStream.schema, props)
//    snsc.sql("select count(*) as count from kafkaStreamGemXdTable").show()

    snsc.sql( """STREAMING CONTEXT START """)
    ssc.awaitTerminationOrTimeout(120* 1000)
    snsc.sql( """STREAMING CONTEXT STOP """)
  }

  def sentiment(s:String) : String = {
    val positive = Array("like", "love", "good", "great", "happy", "cool", "the", "one", "that")
    val negative = Array("hate", "bad", "stupid", "is")

    var st = 0;

    val words = s.split(" ")
    positive.foreach(p =>
      words.foreach(w =>
        if(p==w) st = st+1
      )
    )

    negative.foreach(p=>
      words.foreach(w=>
        if(p==w) st = st-1
      )
    )
    if(st>0)
      "positivie"
    else if(st<0)
      "negative"
    else
      "neutral"
  }

}
