package io.snappydata.app

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
    val conf = new org.apache.spark.SparkConf().
      setMaster("local[2]").
      setAppName("streamingsql")
    ssc = new StreamingContext(new SparkContext(conf), Duration(10000))
    snsc = StreamingSnappyContext(ssc);
  }

  def afterFunction(): Unit = {
    if (ssc != null) {
      ssc.stop()
    }
  }

  before(beforeFunction)
  after(afterFunction)

  test("sql on socket streams") {

    snsc.sql("create stream table socketStreamTable (name string, age int) using socket options (hostname 'localhost', port '9998', " +
      "storagelevel 'MEMORY_AND_DISK_SER_2', formatter 'org.apache.spark.sql.streaming.MyStreamFormatter', converter 'org.apache.spark.sql.streaming.MyStreamConverter')")

//    val tableDStream = snsc.getSchemaDStream("socketStreamTable")
//    tableDStream.registerAsTable("socketStreamTable")

    val resultSet: SchemaDStream = snsc.registerCQ("SELECT name FROM socketStreamTable window (duration '10' seconds, slide '10' seconds) WHERE age >= 18")

    resultSet.foreachRDD {
      r => r.foreach(print)
    }

    snsc.sql( """STREAMING CONTEXT START """)
    ssc.awaitTerminationOrTimeout(20000)
    snsc.sql( """STREAMING CONTEXT STOP """)
  }

 /* test("sql on kafka streams") {

    //snc.sql( """STREAMING CONTEXT  INIT 10""")

    snsc.sql("create stream table kafkaStreamTable (name string, age int) using kafka options (storagelevel 'MEMORY_AND_DISK_SER_2', formatter 'org.apache.spark.sql.streaming.MyStreamFormatter', " +
      " zkQuorum '10.112.195.65:2181', groupId 'streamSQLConsumer', topics 'test:01')")

    snsc.sql("create stream table directKafkaStreamTable (name string, age int) using kafka options (storagelevel 'MEMORY_AND_DISK_SER_2', formatter 'org.apache.spark.sql.streaming.MyStreamFormatter', " +
      " kafkaParams 'metadata.broker.list -> localhost:9092', topics 'test')")

    val resultSet1: SchemaDStream = snsc.registerCQ("SELECT name FROM kafkaStreamTable window (duration '10' seconds, slide '10' seconds) WHERE age >= 18")

    val resultSet2: SchemaDStream = snsc.registerCQ("SELECT name FROM directKafkaStreamTable window (duration '10' seconds, slide '10' seconds) WHERE age >= 18")

    resultSet1.foreachRDD {
      r => r.foreach(print)
    }
    resultSet2.foreachRDD {
      r => r.foreach(print)
    }
    snsc.sql( """STREAMING CONTEXT START """)
    ssc.awaitTerminationOrTimeout(20000)
    snsc.sql( """STREAMING CONTEXT STOP """)
  }
*/
/*  test("sql on file streams") {

    var hfile: String = getClass.getResource("/2015.parquet").getPath

    snsc.sql("create stream table fileStreamTable (id int, name string) using file options (storagelevel 'MEMORY_AND_DISK_SER_2', formatter 'org.apache.spark.sql.streaming.MyStreamFormatter', " +
      //" directory 'tmp')")
      " directory '" + hfile + "')")

    val resultSet: SchemaDStream = snsc.registerCQ("SELECT name FROM fileStreamTable window (duration '10' seconds, slide '10' seconds) WHERE age >= 18")

    resultSet.foreachRDD {
      r => r.foreach(print)
    }
    snsc.sql( """STREAMING CONTEXT START """)
    ssc.awaitTerminationOrTimeout(20000)
    snsc.sql( """STREAMING CONTEXT STOP """)
  }*/
}
