package io.snappydata.app.streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{SchemaDStream, StreamingSnappyContext}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created by ymahajan on 25/09/15.
 */
class StreamingSQLSuite extends FunSuite with Eventually with BeforeAndAfter {

  private var ssc: StreamingContext = null
  private var snsc: StreamingSnappyContext = null

  def beforeFunction(): Unit = {
    val conf = new org.apache.spark.SparkConf()
      .setMaster("local[2]")
      .setAppName("streamingsql")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    ssc = new StreamingContext(new SparkContext(conf), Duration(10000))
    ssc.checkpoint("tmp")
    snsc = StreamingSnappyContext(ssc);
  }

  def afterFunction(): Unit = {
    snsc.sql( """STREAMING CONTEXT STOP """)
  }

  before(beforeFunction)
  after(afterFunction)

  ignore("sql on socket streams") {

    snsc.sql("create stream table socketStreamTable (name string, age int) using socket options (hostname 'localhost', port '9998', " +
      "storagelevel 'MEMORY_AND_DISK_SER_2', formatter 'org.apache.spark.sql.streaming.MyStreamFormatter', converter 'org.apache.spark.sql.streaming.MyStreamConverter')")

    //val tableDStream = snsc.getSchemaDStream("socketStreamTable")

    val resultSet: SchemaDStream = snsc.registerCQ("SELECT name FROM socketStreamTable window (duration '10' seconds, slide '10' seconds) WHERE age >= 18")

    val thrown = intercept[Exception] {
      snsc.sql( """STREAMING CONTEXT START """)
    }
    assert(thrown.getMessage === "requirement failed: No output operations registered, so nothing to execute")
    ssc.awaitTerminationOrTimeout(10000)
  }

  test("sql on kafka streams") {
    intercept[Exception] {
    snsc.sql("create stream table kafkaStreamTable (name string, age int) using kafka options (storagelevel 'MEMORY_AND_DISK_SER_2', formatter 'org.apache.spark.sql.streaming.MyStreamFormatter', " +
      " zkQuorum '10.112.195.65:2181', groupId 'streamSQLConsumer', topics 'tweets:01')")

    snsc.sql("create stream table directKafkaStreamTable (name string, age int) using kafka options (storagelevel 'MEMORY_AND_DISK_SER_2', formatter 'org.apache.spark.sql.streaming.MyStreamFormatter', " +
      " kafkaParams 'metadata.broker.list->localhost:9092,auto.offset.reset->smallest', topics 'tweets')")

    val tableDStream: SchemaDStream = snsc.getSchemaDStream("directKafkaStreamTable")

    val resultSet1: SchemaDStream = snsc.registerCQ("SELECT name FROM kafkaStreamTable window (duration '10' seconds, slide '10' seconds) WHERE age >= 18")
    val resultSet2: SchemaDStream = snsc.registerCQ("SELECT name FROM directKafkaStreamTable window (duration '10' seconds, slide '10' seconds) WHERE age >= 18")

    import org.apache.spark.sql.streaming.snappy._
    val props = Map(
      "url" -> "jdbc:gemfirexd:;mcast-port=33619;user=app;password=app;persist-dd=false",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )

    tableDStream.saveToExternalTable("kafkaStreamGemXdTable", tableDStream.schema, props)
    snsc.sql( """STREAMING CONTEXT START """)
    ssc.awaitTerminationOrTimeout(10000)
    }
  }

  ignore("sql on file streams") {
    var hfile: String = getClass.getResource("/2015.parquet").getPath
    snsc.sql("create stream table fileStreamTable (name string, age int) using file options (storagelevel 'MEMORY_AND_DISK_SER_2', formatter 'org.apache.spark.sql.streaming.MyStreamFormatter', " +
      " directory '" + hfile + "')")
    snsc.registerCQ("SELECT name FROM fileStreamTable window (duration '10' seconds, slide '10' seconds) WHERE age >= 18")
    snsc.sql( """STREAMING CONTEXT START """)
    ssc.awaitTerminationOrTimeout(10000)
  }
}
