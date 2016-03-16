package io.snappydata.benchmark.snappy

import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by kishor on 19/10/15.
 */
object TPCH_Spark {

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("TPCH_Spark")/*.set("snappydata.store.locators","localhost:10334")*/

//    val urlString = s"jdbc:snappydata:;locators=${args(0)}:10334;persist-dd=false;" +
//        "member-timeout=90000;jmx-manager-start=false;enable-time-statistics=true;statistic-sampling-enabled=true"
//
//    //URL 'jdbc:gemfirexd:;mcast-port=45672;persist-dd=false;'
//    val usingOptionString = s"""
//          USING row
//          OPTIONS ()
//              URL '$urlString',
//              Driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver'
//          )"""
//
//    val props = Map(
//      //"url" -> "jdbc:gemfirexd:;mcast-port=45672;persist-dd=false;user=app;password=app",
//      "url" -> urlString,
//      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
//      "poolImpl" -> "tomcat",
//      "poolProperties" -> "maxActive=300"
////      "user" -> "app",
////      "password" -> "app"
//    )

    val usingOptionString = null
    val props = null
    val isResultCollection = false
    val isSnappy = false
    val sc = new SparkContext(conf)
    val snc = new SQLContext(sc)
    val buckets= null

//    var avgFileStream: FileOutputStream = new FileOutputStream(new File(s"Average.out"))
//    var avgPrintStream:PrintStream = new PrintStream(avgFileStream)

    val path = args(0)
    val queries = args(1).split(",")
    val queryPlan = args(2).toBoolean

    TPCHColumnPartitionedTable.createAndPopulateOrderTable(props, snc, path, isSnappy, buckets)
    TPCHColumnPartitionedTable.createAndPopulateLineItemTable(props, snc, path, isSnappy, buckets)
    TPCHReplicatedTable.createPopulateRegionTable(usingOptionString, props, snc, path, isSnappy)
    TPCHReplicatedTable.createPopulateNationTable(usingOptionString, props, snc, path, isSnappy)
    TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, props, snc, path, isSnappy)
//    TPCHRowPartitionedTable.createPopulateCustomerTable(usingOptionString, props, snc, path, isSnappy, buckets)
//    TPCHRowPartitionedTable.createPopulatePartTable(usingOptionString, props, snc, path, isSnappy, buckets)
//    TPCHRowPartitionedTable.createPopulatePartSuppTable(usingOptionString, props, snc, path, isSnappy, buckets)
    TPCHColumnPartitionedTable.createPopulateCustomerTable(usingOptionString, props, snc, path, isSnappy, buckets)
    TPCHColumnPartitionedTable.createPopulatePartTable(usingOptionString, props, snc, path, isSnappy, buckets)
    TPCHColumnPartitionedTable.createPopulatePartSuppTable(usingOptionString, props, snc, path, isSnappy, buckets)

    if (queryPlan) {
      TPCH_Snappy.queryPlan(snc, isSnappy)
    }


    for(i <- 1 to 1) {
      for(query <- queries) {
        query match {
          case "1" => TPCH_Snappy.execute("q1", snc, isResultCollection, isSnappy, i)
          case "2" => TPCH_Snappy.execute("q2", snc, isResultCollection, isSnappy, i) //taking hours to execute in Snappy
          case "3" => TPCH_Snappy.execute("q3", snc, isResultCollection, isSnappy, i)
          case "4" => TPCH_Snappy.execute("q4", snc, isResultCollection, isSnappy, i)
          case "5" => TPCH_Snappy.execute("q5", snc, isResultCollection, isSnappy, i)
          case "6" => TPCH_Snappy.execute("q6", snc, isResultCollection, isSnappy, i)
          case "7" => TPCH_Snappy.execute("q7", snc, isResultCollection, isSnappy, i)
          case "8" => TPCH_Snappy.execute("q8", snc, isResultCollection, isSnappy, i)
          case "9" => TPCH_Snappy.execute("q9", snc, isResultCollection, isSnappy, i) //taking hours to execute in Snappy
          case "10" => TPCH_Snappy.execute("q10", snc, isResultCollection, isSnappy, i)
          case "11" => TPCH_Snappy.execute("q11", snc, isResultCollection, isSnappy, i)
          case "12" => TPCH_Snappy.execute("q12", snc, isResultCollection, isSnappy, i)
          case "13" => TPCH_Snappy.execute("q13", snc, isResultCollection, isSnappy, i)
          case "14" => TPCH_Snappy.execute("q14", snc, isResultCollection, isSnappy, i)
          case "15" => TPCH_Snappy.execute("q15", snc, isResultCollection, isSnappy, i)
          case "16" => TPCH_Snappy.execute("q16", snc, isResultCollection, isSnappy, i)
          case "17" => TPCH_Snappy.execute("q17", snc, isResultCollection, isSnappy, i)
          case "18" => TPCH_Snappy.execute("q18", snc, isResultCollection, isSnappy, i)
          case "19" => TPCH_Snappy.execute("q19", snc, isResultCollection, isSnappy, i) //not working in local mode hence not executing it for cluster mode too
          case "20" => TPCH_Snappy.execute("q20", snc, isResultCollection, isSnappy, i)
          case "21" => TPCH_Snappy.execute("q21", snc, isResultCollection, isSnappy, i) //not working in local mode hence not executing it for cluster mode too
          case "22" => TPCH_Snappy.execute("q22", snc, isResultCollection, isSnappy, i)
            println("---------------------------------------------------------------------------------")
        }
      }
    }

//    TPCH_Snappy.execute("q1", snc, isResultCollection, isSnappy,1)
//    //TPCH_Snappy.execute("q2", snc,isResultCollection, isSnappy, 1)
//    TPCH_Snappy.execute("q3", snc, isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q4", snc,isResultCollection, isSnappy, 1)
//    TPCH_Snappy.execute("q5", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q6", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q7", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q8", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q9", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q10", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q11", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q12", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q13", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q14", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q15", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q16", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q17", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q18", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q19", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy.execute("q20", snc,isResultCollection, isSnappy,1)
//    //TPCH_Snappy_Query.execute("q21", sc,isResultCollection, isSnappy)
//    TPCH_Snappy.execute("q22", snc,isResultCollection, isSnappy,1)
//    TPCH_Snappy_Query.execute("q1s", sc, isResultCollection, isSnappy)
//    TPCH_Snappy_Query.execute("q3s", sc, isResultCollection, isSnappy)
//    TPCH_Snappy_Query.execute("q5s", sc,isResultCollection, isSnappy)
//    TPCH_Snappy_Query.execute("q6s", sc,isResultCollection, isSnappy)
//    TPCH_Snappy_Query.execute("q10s", sc,isResultCollection, isSnappy)

//
//    while(true){
//
//    }

    TPCH_Snappy.close()
    sc.stop()

}
}


