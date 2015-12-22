package io.snappydata.benchmark.local

import io.snappydata.benchmark.{TPCH_Snappy_Query, TPCHReplicatedTable, TPCHRowPartitionedTable, TPCHColumnPartitionedTable}

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by kishor on 19/10/15.
 */
object TPCH_Snappy {

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("TPCH_Snappy").setMaster("local[4]")/*.set("snappydata.store.locators","localhost:10334")*/

    val urlString = "jdbc:snappydata:;locators=localhost:10334;persist-dd=false;" +
        "member-timeout=90000;jmx-manager-start=false;enable-time-statistics=true;statistic-sampling-enabled=true"

    //URL 'jdbc:gemfirexd:;mcast-port=45672;persist-dd=false;'
    val usingOptionString = s"""
          USING row
          OPTIONS (
              URL '$urlString',
              Driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver'
          )"""

    val props = Map(
      //"url" -> "jdbc:gemfirexd:;mcast-port=45672;persist-dd=false;user=app;password=app",
      "url" -> urlString,
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "poolProperties" -> "maxActive=300",
      "user" -> "app",
      "password" -> "app"
    )

    val isResultCollection = args(0).toBoolean
    val isSnappy = args(1).toBoolean
    val path = args(2)
    val sc = new SparkContext(conf)


    TPCHColumnPartitionedTable.createAndPopulateOrderTable(props, sc, path, isSnappy)
    TPCHColumnPartitionedTable.createAndPopulateLineItemTable(props, sc, path, isSnappy)
    TPCHRowPartitionedTable.createPopulateCustomerTable(usingOptionString, props, sc, path, isSnappy)
    TPCHReplicatedTable.createPopulateRegionTable(usingOptionString, props, sc, path, isSnappy)
    TPCHReplicatedTable.createPopulateNationTable(usingOptionString, props, sc, path, isSnappy)
    TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, props, sc, path, isSnappy)
    TPCHRowPartitionedTable.createPopulatePartTable(usingOptionString, props, sc, path, isSnappy)
    TPCHRowPartitionedTable.createPopulatePartSuppTable(usingOptionString, props, sc, path, isSnappy)
    TPCHColumnPartitionedTable.createAndPopulateLineItemSampledTable(props, sc, path)
    TPCHColumnPartitionedTable.createAndPopulateOrderSampledTable(props, sc, path)

    TPCH_Snappy_Query.execute("q1s", sc, isResultCollection, isSnappy)
    TPCH_Snappy_Query.execute("q3s", sc, isResultCollection, isSnappy)
    //TPCH_Snappy_Query.execute("q5s", sc,isResultCollection, isSnappy)
    //TPCH_Snappy_Query.execute("q6s", sc,isResultCollection, isSnappy)
    //TPCH_Snappy_Query.execute("q10s", sc,isResultCollection, isSnappy)

    //TPCH_Snappy_Query.queryPlan(sc, isSnappy)

    TPCH_Snappy_Query.execute("q1", sc, isResultCollection, isSnappy) //RUN SUCCESSFULLY
    TPCH_Snappy_Query.execute("q2", sc,isResultCollection, isSnappy) //EXCEPTION DUE TO SELECT IN WHERE CLAUSE
    TPCH_Snappy_Query.execute("q3", sc, isResultCollection, isSnappy) //RUN BUT DOES NOT RETURN ANY RESULT
    TPCH_Snappy_Query.execute("q4", sc,isResultCollection, isSnappy) //EXCEPTION DUE TO SELECT IN WHERE CLAUSE
    TPCH_Snappy_Query.execute("q5", sc,isResultCollection, isSnappy) //RUN BUT DOES NOT RETURN ANY RESULT
    TPCH_Snappy_Query.execute("q6", sc,isResultCollection, isSnappy) //RUN SUCCESSFULLY
    TPCH_Snappy_Query.execute("q7", sc,isResultCollection, isSnappy) //RUN BUT DOES NOT RETURN ANY RESULT
    TPCH_Snappy_Query.execute("q8", sc,isResultCollection, isSnappy) //RUN BUT DOES NOT RETURN ANY RESULT
    //TPCH_Snappy_Query.execute("q9", sc,isResultCollection, isSnappy) //RUN SUCCESSFULLY
    TPCH_Snappy_Query.execute("q10", sc,isResultCollection, isSnappy) //RUN SUCCESSFULLY
    TPCH_Snappy_Query.execute("q11", sc,isResultCollection, isSnappy) //EXCEPTION DUE TO SELECT IN WHERE CLAUSE WITH sum
    TPCH_Snappy_Query.execute("q12", sc,isResultCollection, isSnappy) //RUN SUCCESSFULLY
    TPCH_Snappy_Query.execute("q13", sc,isResultCollection, isSnappy)  //EXCEPTION DUE TO EXPRESSION  o_comment not like ‘%special%requests%
    TPCH_Snappy_Query.execute("q14", sc,isResultCollection, isSnappy) //RUN SUCCESSFULLY
    TPCH_Snappy_Query.execute("q15", sc,isResultCollection, isSnappy)
    TPCH_Snappy_Query.execute("q16", sc,isResultCollection, isSnappy) //EXCEPTION DUE TO SELECT IN WHERE CLAUSE WITH sum
    TPCH_Snappy_Query.execute("q17", sc,isResultCollection, isSnappy) //EXCEPTION DUE TO SELECT IN WHERE CLAUSE WITH sum
    TPCH_Snappy_Query.execute("q18", sc,isResultCollection, isSnappy) //EXCEPTION DUE TO SELECT IN WHERE CLAUSE WITH sum
    //TPCH_Snappy_Query.execute("q19", sc,isResultCollection, isSnappy) //MUST BE EASY TO FIX
    TPCH_Snappy_Query.execute("q20", sc,isResultCollection, isSnappy)
    //TPCH_Snappy_Query.execute("q21", sc,isResultCollection, isSnappy)
    TPCH_Snappy_Query.execute("q22", sc,isResultCollection, isSnappy)


    if(!isResultCollection) {
      TPCH_Snappy_Query.close()
    }
    sc.stop()

}
}


