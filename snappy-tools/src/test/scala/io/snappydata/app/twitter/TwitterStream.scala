package io.snappydata.app.twitter

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{MessageToRowConverter, StreamUtils, StreamingSnappyContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by ymahajan on 28/10/15.
 */
object TwitterStream {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("StreamingSql")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
      .setMaster("local[2]")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("tmp")
    val snsc = StreamingSnappyContext(ssc)

    snsc.sql("create stream table socketStreamTable (name string) using " +
      "socket_stream options (hostname 'localhost', port '9898', " +
      //"converter 'org.apache.spark.sql.streaming.MyStreamConverter', " +
      "storagelevel 'MEMORY_AND_DISK_SER', streamToRow 'io.snappydata.app.twitter.SocketToRowConverter')")

    val resultSet = snsc.registerCQ("SELECT name FROM socketStreamTable window " +
      "(duration '1' seconds, slide '1' seconds)")

    resultSet.foreachRDD(rdd =>{ rdd.foreach(row => println("YOGS" +row))})

    //    val tableStream = snsc.getSchemaDStream("socketstreamtable")
//    tableStream.map(x  => x+"yogs").print()

//    val clz = StreamUtils.loadClass("io.snappydata.app.twitter.SocketToRowConverter")
//    val streamToRow = clz.newInstance().asInstanceOf[MessageToRowConverter]
//    val words = tableStream.map(streamToRow.toRow)
//    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
//    wordCounts.print()

    //    snsc.sql("DROP TABLE IF EXISTS columnTable")
    //    snsc.sql("CREATE TABLE columnTable (Col1 INT, Col2 INT, Col3 INT) USING column OPTIONS (PARTITION_BY 'col1')")
    //    val result = snsc.sql("SELECT * FROM columnTable")
    //    val r = result.collect
    //    assert(r.length == 0)
    //    println("Successful")
    //
    //    var hfile: String = "/tmp/testdata"
    //    // Create stream table using sql
    //    snsc.sql("CREATE stream TABLE fileStreamTable (name string) USING file_stream OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2', streamToRow 'io.snappydata.app.twitter.LineToRowConverter', " +
    //      " directory '" + hfile + "')")
    //    // Registration of CQ
    //
    //    val reslult = snsc.registerCQ("SELECT name FROM fileStreamTable window (duration '10' seconds, slide '10' seconds)")
    //    reslult.foreachRDD(rdd =>
    //      {println( if(!rdd.isEmpty()) rdd.first else "emptyrdd")}
    //    )

    /*    val sparkConf = new org.apache.spark.SparkConf()
          .setAppName("TwitterStream")
        .setMaster("local[2]")
        //.set("snappydata.embedded", "true")

        val sc = new SparkContext(sparkConf)
        val ssc = new StreamingContext(sc, Milliseconds(30000))
        //SnappyContext.getOrCreate(sc)
        val ssnc = StreamingSnappyContext(ssc)
        val filters = Array("", "");
        val stream = TwitterUtils.createStream(ssc, None, filters, StorageLevel.MEMORY_ONLY_SER_2)

    //    val schema = StructType(Array(StructField("tweet",StringType)))
    //
    //   val stream = ssnc.createSchemaDStream(stream.asInstanceOf[DStream[Any]], schema)
        stream.foreachRDD(rdd => {println(rdd)})
        //val users = stream.map(status => println(status))*/
    ssc.start()
    ssc.awaitTerminationOrTimeout(30 * 1000)
    ssc.stop()
    println("ok")
  }

}