package io.snappydata.app
package io.snappydata.app

import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.streaming.{StreamingSnappyContext, KafkaStreamSource, KafkaStreamRelation}

object KafkaStreamingSQLTest {
  def main(args: Array[String]) {
    println("start")

    val conf = new org.apache.spark.SparkConf().
      setMaster("local[2]").
      setAppName("StreamingSQLTest").
      set("spark.driver.cores", "2").
      set("spark.executor.memory", "8g").
      set("spark.driver.memory", "8g")

    val sc = new org.apache.spark.SparkContext(conf)

    val snc = SnappyContext(sc)
    snc.sql( """STREAMING CONTEXT  INIT 10""")

    //    snc.sql("create stream table socketStreamTable (id int, name string) using socket options (hostname 'localhost', port '9998', "+
    //       "storagelevel 'MEMORY_AND_DISK_SER_2', formatter 'org.apache.spark.sql.streaming.MyStreamFormatter', converter 'org.apache.spark.sql.streaming.MyStreamConverter')")

        snc.sql("create stream table kafkaStreamTable (id int, name string) using kafka options (storagelevel 'MEMORY_AND_DISK_SER_2', formatter 'org.apache.spark.sql.streaming.MyStreamFormatter', " +
          " zkQuorum '10.112.195.65:2181', groupId 'streamSQLConsumer', topics 'test:01')")

//    snc.sql("create stream table kafkaStreamTable (id int, name string) using kafka options (storagelevel 'MEMORY_AND_DISK_SER_2', formatter 'org.apache.spark.sql.streaming.MyStreamFormatter', " +
//      " kafkaParams 'metadata.broker.list -> localhost:9092', topics 'test')")

    //    snc.sql("create stream table fileStreamTable (id int, name string) using file options (storagelevel 'MEMORY_AND_DISK_SER_2', formatter 'org.apache.spark.sql.streaming.MyStreamFormatter', " +
    //      " directory 'temp')")

    snc.sql( """STREAMING CONTEXT START """)
    //    snc.sql("select * from kafkaStreamTable").show()
    snc.sql("select count(*) from kafkaStreamGemXdTable").show()
    //    snc.sql("SELECT t.name, COUNT(t.name) FROM (SELECT * FROM kafkaStreamTable) AS t GROUP BY t.name").show()
    //foreachRDD { r => r.foreach(println) }

    //snc.sql("select * from socketStreamTable").show()
    //snc.sql("select * from kafkaStreamTable").show()
    //snc.sql("select * from fileStreamTable").show()

    Thread.sleep(10000)

    //snc.sql("select count(*) from socketStreamTable").show()
    //snc.sql("select count(*) from kafkaStreamTable").show()
    //snc.sql("SELECT t.name, COUNT(t.name) FROM (SELECT * FROM kafkaStreamTable) AS t GROUP BY t.name").show()
    //snc.sql("select count(*) from fileStreamTable").show()

    Thread.sleep(5000)

    //  } onSuccess { case ret => println("YAHOOOOO!!!!...." + ret) }

    //Thread.sleep(30000)

    //snc.sql( """STREAMING CONTEXT STOP """)
    println("end")
  }
}



