package io.snappydata.app.twitter

import org.apache.spark._
import org.apache.spark.rdd.RDD
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

    val sc = new org.apache.spark.SparkConf().
      setMaster("local[2]").
      setAppName("streamingsql")
    val ssc = new StreamingContext(new SparkContext(sc), Duration(10000))
    val snsc = StreamingSnappyContext(ssc);

    val streamTable = "directKafkaStreamTable"
    snsc.sql("create stream table "+ streamTable + " (name string, text string) using kafka options (storagelevel 'MEMORY_AND_DISK_SER_2', formatter 'org.apache.spark.sql.streaming.MyStreamFormatter', " +
      " kafkaParams 'metadata.broker.list->localhost:9092', topics 'tweets')")

    //snsc.sql("TRUNCATE TABLE " + streamTable)

    //snsc.sql("DROP TABLE " + streamTable)

    //val tableDStream: SchemaDStream = snsc.getSchemaDStream("directKafkaStreamTable")
    //import org.apache.spark.sql.streaming.snappy._
    //tableDStream.saveToExternalTable("kafkaStreamGemXdTable", tableDStream.schema, props)

    val resultSet: SchemaDStream = snsc.registerCQ("SELECT name FROM directKafkaStreamTable window (duration '10' seconds, slide '10' seconds) ") //WHERE age >= 18

    val storeTable = "directKafkaStoreTable"
    val storeTable2 = "storeTable2"

    snsc.sql("CREATE TABLE " + storeTable2 + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " +
      options)

    snsc.dropExternalTable(storeTable2, false)

    snsc.sql("CREATE TABLE " + storeTable + " USING column " +
      options + " AS (SELECT * FROM " + streamTable + ")")

    snsc.sql("SELECT * FROM " + storeTable).show()

    snsc.sql("DROP TABLE " + storeTable)

    snsc.sql( """STREAMING CONTEXT START """)
    ssc.awaitTerminationOrTimeout(20000)
    snsc.sql( """STREAMING CONTEXT STOP """)
  }
}
