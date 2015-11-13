package io.snappydata.app.twitter

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.RDDConversions
import org.apache.spark.sql.streaming.{SchemaDStream, StreamingSnappyContext}
import org.apache.spark.streaming._
import org.apache.spark.sql.{AnalysisException, SaveMode, SnappyContext}

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
      .set("snappydata.store.locators", "localhost")
    val ssc = new StreamingContext(new SparkContext(sc), Milliseconds(200))
    val snsc = StreamingSnappyContext(ssc);

    val streamTable = "directKafkaStreamTable"
    snsc.sql("create stream table "+ streamTable + " (id long, text string, fullName string, country string, retweets int) using kafka_stream options (storagelevel 'MEMORY_AND_DISK_SER_2', streamToRow 'io.snappydata.app.twitter.KafkaMessageToRowConverter' ," +
      " kafkaParams 'metadata.broker.list->localhost:9092', topics 'tweetstream')")

    snsc.sql("create sampled table directKafkaStreamTable_sampled Options (basetable 'directKafkaStreamTable', qcs 'text', fraction '0.005', strataReservoirSize '100' )")
    snsc.cacheTable("directKafkaStreamTable_sampled")

    val tableStream = snsc.getSchemaDStream(streamTable)
    //val tableSchema = snsc.getSchema(streamTable)
    snsc.dropExternalTable("kafkaExtTable", true)
    snsc.createExternalTable("kafkaExtTable" , "column", tableStream.schema, Map.empty[String,String])

    tableStream.foreachRDD(rdd =>
      println(s"Saving Twitter stream:" +
        s" ${
          //snsc.appendToCacheRDD(rdd, "kafkaExtTable", tableStream.schema)
          val df = snsc.createDataFrame(rdd, tableStream.schema)
          //df.show()
          df.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("kafkaExtTable")
        }")
    )
//    import StreamingSnappyContext._
//    tableStream.saveToExternalTable("kafkaExtTable", tableStream.schema, Map.empty[String,String])

//    tableStream.foreachRDD(rdd => {
//      val df = snsc.createDataFrame(rdd, tableStream.schema)
//      //df.show()
//      //df.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("kafkaExtTable")
//    }
//    )
//    val result = snsc.sql("SELECT * FROM kafkaExtTable")
//    val r = result.collect
//    System.out.println("YOGS result ",r.length)

    //snsc.cacheTable(streamTable)
    //snsc.udf.register("sentiment", sentiment _)

    val query1 = "SELECT * FROM directKafkaStreamTable window (duration '2' seconds, slide '2' seconds) where country='jp'"
    val query2 = "SELECT * FROM directKafkaStreamTable window (duration '2' seconds, slide '2' seconds) where text like '%girl%' limit 10"
    val query3 = "SELECT retweets, max(retweets), min(retweets), avg(retweets) FROM directKafkaStreamTable window (duration '20' seconds, slide '20' seconds) group by retweets"
    val query4 = "SELECT sentiment(text) FROM directKafkaStreamTable window (duration '10' seconds, slide '10' seconds) where text like '%girl%' group by sentiment(text)"
    val query5 = "SELECT retweets, max(retweets), min(retweets), avg(retweets) FROM directKafkaStreamTable window (duration '4' seconds, slide '4' seconds) group by retweets"

    val resultSet: SchemaDStream = snsc.registerCQ("select * from directKafkaStreamTable " +
      "window (duration '4' seconds, slide '4' seconds) d, kafkaExtTable k where d.id=k.id and d.text like '%girl%'")
    resultSet.printSchema()
    resultSet.explain()
    println("columns ", resultSet.columns)
    resultSet.foreachRDD(rdd =>
      println(s"Received Twitter stream results. Count:" +
        s" ${if(!rdd.isEmpty()) {
          val df = snsc.createDataFrame(rdd, resultSet.schema)
          df.show()
          //df.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("kafkaExtTable")
          //snsc.appendToCacheRDD(rdd, streamTable, resultSet.schema)
        }
        }")
    )
    //resultSet.foreachRDD { r => r.foreach(println) }

    //Thread.sleep(40000)

    snsc.sql("select * from kafkaExtTable").show()
    //snsc.sql("select * from directKafkaStreamTable_sampled").show()

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
    ssc.awaitTerminationOrTimeout(30* 1000)
    snsc.sql( """STREAMING CONTEXT STOP """)
    //snsc.sql("select * from directKafkaStreamTable_sampled").show()
    snsc.sql("select * from kafkaExtTable").show()

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
