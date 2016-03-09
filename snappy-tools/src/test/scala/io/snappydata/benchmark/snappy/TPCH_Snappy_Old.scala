package io.snappydata.benchmark.snappy

import io.snappydata.benchmark.{TPCHReplicatedTable, TPCHRowPartitionedTable, TPCHColumnPartitionedTable}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by kishor on 19/10/15.
 */
object TPCH_Snappy_Old {

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("TPCH_Snappy")/*.set("snappydata.store.locators","localhost:10334")*/

    val urlString = s"jdbc:snappydata:;locators=${args(0)}:10334;persist-dd=false;" +
        "member-timeout=90000;jmx-manager-start=false;enable-time-statistics=true;statistic-sampling-enabled=true"

    //URL 'jdbc:gemfirexd:;mcast-port=45672;persist-dd=false;'
    val usingOptionString = s"""
          USING row
          OPTIONS ()
              URL '$urlString',
              Driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver'
          )"""

    val props = Map(
      //"url" -> "jdbc:gemfirexd:;mcast-port=45672;persist-dd=false;user=app;password=app",
      "url" -> urlString,
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "poolProperties" -> "maxActive=300"
//      "user" -> "app",
//      "password" -> "app"
    )

    val isResultCollection = false
    val isSnappy = false
    val path = args(2)
    val sc = new SparkContext(conf)
    val snc = new SQLContext(sc)
    val buckets="113"

    TPCHColumnPartitionedTable.createAndPopulateOrderTable(props, snc, path, isSnappy, buckets)
    TPCHColumnPartitionedTable.createAndPopulateLineItemTable(props, snc, path, isSnappy, buckets)
    TPCHRowPartitionedTable.createPopulateCustomerTable(usingOptionString, props, snc, path, isSnappy, buckets)
    TPCHReplicatedTable.createPopulateRegionTable(usingOptionString, props, snc, path, isSnappy)
    TPCHReplicatedTable.createPopulateNationTable(usingOptionString, props, snc, path, isSnappy)
    TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, props, snc, path, isSnappy)
    TPCHRowPartitionedTable.createPopulatePartTable(usingOptionString, props, snc, path, isSnappy, buckets)
    TPCHRowPartitionedTable.createPopulatePartSuppTable(usingOptionString, props, snc, path, isSnappy, buckets)

//    TPCH_Snappy_Query.execute("q1s", sc, isResultCollection, isSnappy)
//    TPCH_Snappy_Query.execute("q3s", sc, isResultCollection, isSnappy)
//    //TPCH_Snappy_Query.execute("q5s", sc,isResultCollection, isSnappy)
    //TPCH_Snappy_Query.execute("q6s", sc,isResultCollection, isSnappy)
    //TPCH_Snappy_Query.execute("q10s", sc,isResultCollection, isSnappy)

    //TPCH_Snappy_Query.queryPlan(sc, isSnappy)

    TPCH_Snappy.execute("q1", snc, isResultCollection, isSnappy,1)
    //TPCH_Snappy.execute("q2", snc,isResultCollection, isSnappy, 1)
    TPCH_Snappy.execute("q3", snc, isResultCollection, isSnappy,1)
    TPCH_Snappy.execute("q4", snc,isResultCollection, isSnappy, 1)
    TPCH_Snappy.execute("q5", snc,isResultCollection, isSnappy,1)
    TPCH_Snappy.execute("q6", snc,isResultCollection, isSnappy,1)
    TPCH_Snappy.execute("q7", snc,isResultCollection, isSnappy,1)
    TPCH_Snappy.execute("q8", snc,isResultCollection, isSnappy,1)
    TPCH_Snappy.execute("q9", snc,isResultCollection, isSnappy,1)
    TPCH_Snappy.execute("q10", snc,isResultCollection, isSnappy,1)
    TPCH_Snappy.execute("q11", snc,isResultCollection, isSnappy,1)
    TPCH_Snappy.execute("q12", snc,isResultCollection, isSnappy,1)
    TPCH_Snappy.execute("q13", snc,isResultCollection, isSnappy,1)
    TPCH_Snappy.execute("q14", snc,isResultCollection, isSnappy,1)
    TPCH_Snappy.execute("q15", snc,isResultCollection, isSnappy,1)
    TPCH_Snappy.execute("q16", snc,isResultCollection, isSnappy,1)
    TPCH_Snappy.execute("q17", snc,isResultCollection, isSnappy,1)
    TPCH_Snappy.execute("q18", snc,isResultCollection, isSnappy,1)
    //TPCH_Snappy_Query.execute("q19", sc,isResultCollection, isSnappy)
    TPCH_Snappy.execute("q20", snc,isResultCollection, isSnappy,1)
    //TPCH_Snappy_Query.execute("q21", sc,isResultCollection, isSnappy)
    TPCH_Snappy.execute("q22", snc,isResultCollection, isSnappy,1)

    //TPCH_Snappy.queryPlan(snc, isSnappy)

    while(true){

    }

    if(!isResultCollection) {
      TPCH_Snappy.close()
    }
    sc.stop()

}
}


