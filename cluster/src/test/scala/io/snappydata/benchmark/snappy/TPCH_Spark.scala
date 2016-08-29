package io.snappydata.benchmark.snappy

import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by kishor on 19/10/15.
 */
object TPCH_Spark {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TPCH_Spark") /*.set("snappydata.store.locators","localhost:10334")*/

    val usingOptionString = null
    val props = null
    var isSnappy = false
    val sc = new SparkContext(conf)
    val snc = new SQLContext(sc)
    val buckets = null
    val useIndex = false

    val path = args(0)
    val queries = args(1).split("-")
    var isResultCollection : Boolean = args(2).toBoolean
    var warmup : Integer = args(3).toInt
    var runsForAverage : Integer = args(4).toInt
    var sqlSparkProperties = args(5).split(",")


    TPCHColumnPartitionedTable.createAndPopulateOrderTable(props, snc, path, isSnappy, buckets)
    TPCHColumnPartitionedTable.createAndPopulateLineItemTable(props, snc, path, isSnappy, buckets)
    TPCHReplicatedTable.createPopulateRegionTable(usingOptionString, props, snc, path, isSnappy)
    TPCHReplicatedTable.createPopulateNationTable(usingOptionString, props, snc, path, isSnappy)
    TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, props, snc, path, isSnappy)
    TPCHColumnPartitionedTable.createPopulateCustomerTable(usingOptionString, props, snc, path, isSnappy, buckets)
    TPCHColumnPartitionedTable.createPopulatePartTable(usingOptionString, props, snc, path, isSnappy, buckets)
    TPCHColumnPartitionedTable.createPopulatePartSuppTable(usingOptionString, props, snc, path, isSnappy, buckets)

//    snc.sql(s"set spark.sql.shuffle.partitions=83")
//    snc.sql(s"set spark.sql.inMemoryColumnarStorage.compressed=false")
//    snc.sql(s"set spark.sql.autoBroadcastJoinThreshold=41943040")
    //snc.sql(s"set spark.sql.crossJoin.enabled = true")
    for(prop <- sqlSparkProperties) {
      println(prop)
      snc.sql(s"set $prop")
    }

    for (i <- 1 to 1) {
      for (query <- queries) {
        query match {
          case "1" => TPCH_Snappy.execute("q1", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "2" => TPCH_Snappy.execute("q2", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "3" => TPCH_Snappy.execute("q3", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "4" => TPCH_Snappy.execute("q4", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "5" => TPCH_Snappy.execute("q5", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "6" => TPCH_Snappy.execute("q6", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "7" => TPCH_Snappy.execute("q7", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "8" => TPCH_Snappy.execute("q8", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "9" => TPCH_Snappy.execute("q9", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "10" => TPCH_Snappy.execute("q10", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "11" => TPCH_Snappy.execute("q11", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "12" => TPCH_Snappy.execute("q12", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "13" => TPCH_Snappy.execute("q13", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "14" => TPCH_Snappy.execute("q14", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "15" => TPCH_Snappy.execute("q15", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "16" => TPCH_Snappy.execute("q16", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "17" => TPCH_Snappy.execute("q17", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "18" => TPCH_Snappy.execute("q18", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "19" => TPCH_Snappy.execute("q19", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "20" => TPCH_Snappy.execute("q20", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "21" => TPCH_Snappy.execute("q21", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
          case "22" => TPCH_Snappy.execute("q22", snc, isResultCollection, isSnappy, i, useIndex, warmup, runsForAverage)
            println("---------------------------------------------------------------------------------")
        }
      }
    }
    TPCH_Snappy.close()
    sc.stop()

  }
}


