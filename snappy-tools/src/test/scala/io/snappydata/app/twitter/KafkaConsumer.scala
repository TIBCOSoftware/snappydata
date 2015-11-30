package io.snappydata.app.twitter

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.{SchemaDStream, StreamingSnappyContext}
import org.apache.spark.streaming._
import org.apache.spark.sql.SaveMode

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
//  val options =  "OPTIONS (url 'jdbc:gemfirexd:;mcast-port=33619;user=app;password=app;persist-dd=false' ," +
//    "driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver' ," +
//    "poolimpl 'tomcat', " +
//    "user 'app', " +
//    "password 'app' ) "


  def main(args: Array[String]) {
    val sparkConf = new org.apache.spark.SparkConf()
     .setMaster("local[2]")
      //.setMaster("snappydata://localhost:10334")
      .setAppName("kafkaconsumer")
      .set("snappydata.store.locators", "localhost")
    //.set("snappydata.store.locators", "localhost:1527")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Milliseconds(200))
    ssc.remember(Milliseconds(60000+1))
    ssc.checkpoint("/home/ymahajan/checkpoint")
    val ssnc = StreamingSnappyContext(ssc);

    val streamTable = "directKafkaStreamTable"
    ssnc.sql("create stream table "+ streamTable + " (id long, text string, fullName string, country string, retweets int) " +
      "using kafka_stream options " +
      "(storagelevel 'MEMORY_AND_DISK_SER_2', streamToRow 'io.snappydata.app.twitter.KafkaMessageToRowConverter' ," +
      " kafkaParams 'metadata.broker.list->localhost:9092', topics 'tweetstream')")

    ssnc.sql("create sampled table directKafkaStreamTable_sampled Options " +
      "(basetable 'directKafkaStreamTable', qcs 'text', fraction '0.005', strataReservoirSize '100' )")
    ssnc.cacheTable("directKafkaStreamTable_sampled")

    val tableStream = ssnc.getSchemaDStream(streamTable)

//    tableStream.
    ssnc.dropExternalTable("kafkaExtTable", true)
    ssnc.createExternalTable("kafkaExtTable" , "column", tableStream.schema, Map.empty[String,String])

    tableStream.foreachRDD(rdd =>
      //println(s"Saving Twitter stream:" +
      println(s"" +
        s" ${
          val df = ssnc.createDataFrame(rdd, tableStream.schema)
          //df.show
          df.write.format("column").mode(SaveMode.Append).options(Map.empty[String,String]).saveAsTable("kafkaExtTable")
        }")
    )
    ssnc.sql( """STREAMING CONTEXT START """)

    val resultSet: SchemaDStream = ssnc.registerCQ("SELECT * FROM directKafkaStreamTable window (duration '2' seconds, slide '2' seconds) where text like '%the%'")
    //    val resultSet: SchemaDStream = ssnc.registerCQ("select * from directKafkaStreamTable " +
    //      "window (duration '4' seconds, slide '4' seconds) d, kafkaExtTable k where d.id=k.id and d.text like '%girl%'")
    resultSet.foreachRDD(rdd =>
      println(s"Received Twitter stream results. Count:" +
        s" ${if(!rdd.isEmpty()) {
          val df = ssnc.createDataFrame(rdd, resultSet.schema)
          df.show
          //df.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("kafkaExtTable")
          //ssnc.appendToCacheRDD(rdd, streamTable, resultSet.schema)
        }
        }")
    )
    Thread.sleep(12344)
    val resultSet2: SchemaDStream = ssnc.registerCQ("SELECT * FROM directKafkaStreamTable window (duration '2' seconds, slide '2' seconds) where text like '%the%'")
    //    val resultSet: SchemaDStream = ssnc.registerCQ("select * from directKafkaStreamTable " +
    //      "window (duration '4' seconds, slide '4' seconds) d, kafkaExtTable k where d.id=k.id and d.text like '%girl%'")
    resultSet2.foreachRDD(rdd =>
      println(s"Received Twitter stream results. Count:" +
        s" ${if(!rdd.isEmpty()) {
          val df = ssnc.createDataFrame(rdd, resultSet.schema)
          df.show
          //df.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("kafkaExtTable")
          //ssnc.appendToCacheRDD(rdd, streamTable, resultSet.schema)
        }
        }")
    )


    //    tableStream.foreachRDD(rdd =>
//      println(s"Saving Twitter stream:" +
//        s" ${
//          //ssnc.appendToCacheRDD(rdd, "kafkaExtTable", tableStream.schema)
//          //OR
//          val df = ssnc.createDataFrame(rdd, tableStream.schema)
//          df.write.format("row").mode(SaveMode.Append).options(Map.empty[String,String]).saveAsTable("kafkaExtTable")
//        }")
//    )

//    import StreamingSnappyContext._
//    tableStream.saveToExternalTable("kafkaExtTable", tableStream.schema, Map.empty[String,String])

//    tableStream.foreachRDD(rdd => {
//      val df = ssnc.createDataFrame(rdd, tableStream.schema)
//      //df.show()
//      //df.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("kafkaExtTable")
//    }
//    )
//    val result = ssnc.sql("SELECT * FROM kafkaExtTable")
//    val r = result.collect
//    System.out.println("YOGS result ",r.length)

    //ssnc.cacheTable(streamTable)

    //ssnc.udf.register("sentiment", sentiment _)

//    val query1 = "SELECT * FROM directKafkaStreamTable window (duration '2' seconds, slide '2' seconds) where country='jp'"
//    val query2 = "SELECT * FROM directKafkaStreamTable window (duration '2' seconds, slide '2' seconds) where text like '%girl%' limit 10"
//    val query3 = "SELECT retweets, max(retweets), min(retweets), avg(retweets) FROM directKafkaStreamTable window (duration '20' seconds, slide '20' seconds) group by retweets"
//    val query4 = "SELECT sentiment(text) FROM directKafkaStreamTable window (duration '10' seconds, slide '10' seconds) where text like '%girl%' group by sentiment(text)"
//    val query5 = "SELECT retweets, max(retweets), min(retweets), avg(retweets) FROM directKafkaStreamTable window (duration '4' seconds, slide '4' seconds) group by retweets"

   // ssnc.sql( """STREAMING CONTEXT START """)


//    val resultSet: SchemaDStream = ssnc.registerCQ("SELECT * FROM directKafkaStreamTable window (duration '2' seconds, slide '2' seconds) where text like '%girl%'")
////    val resultSet: SchemaDStream = ssnc.registerCQ("select * from directKafkaStreamTable " +
////      "window (duration '4' seconds, slide '4' seconds) d, kafkaExtTable k where d.id=k.id and d.text like '%girl%'")
//    resultSet.foreachRDD(rdd =>
//      println(s"Received Twitter stream results. Count:" +
//        s" ${if(!rdd.isEmpty()) {
//          val df = ssnc.createDataFrame(rdd, resultSet.schema)
//          df.show()
//          //df.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("kafkaExtTable")
//          //ssnc.appendToCacheRDD(rdd, streamTable, resultSet.schema)
//        }
//        }")
//    )


        resultSet.foreachRDD(rdd =>
            {
              val df = ssnc.createDataFrame(rdd, resultSet.schema)
              df.show()
              df.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("kafkaExtTable")
            }
        )
    //resultSet.foreachRDD { r => r.foreach(println) }

    //Thread.sleep(40000)

    //ssnc.sql("select * from kafkaExtTable").show()
    //ssnc.sql("select * from directKafkaStreamTable_sampled").show()

//    val directKafkaStoreTable = "directKafkaStoreTable"
//    ssnc.sql("CREATE TABLE " + directKafkaStoreTable + " USING column " +
//      options + " AS (SELECT * FROM " + streamTable + ")")
//    ssnc.sql("SELECT * FROM " + directKafkaStoreTable).show()
//    ssnc.sql("DROP TABLE " + directKafkaStoreTable)

//    val tableDStream: SchemaDStream = ssnc.getSchemaDStream("directKafkaStreamTable")
//    import org.apache.spark.sql.streaming.snappy._
//    tableDStream.saveToExternalTable("kafkaStreamGemXdTable", tableDStream.schema, props)
//    ssnc.sql("select count(*) as count from kafkaStreamGemXdTable").show()

    ssc.awaitTerminationOrTimeout(300* 1000)
    ssnc.sql( """STREAMING CONTEXT STOP """)
    //ssnc.sql("select * from directKafkaStreamTable_sampled").show()
    //ssnc.sql("select * from kafkaExtTable").show()

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
