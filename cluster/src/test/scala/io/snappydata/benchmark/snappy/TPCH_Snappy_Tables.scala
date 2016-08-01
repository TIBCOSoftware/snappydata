package io.snappydata.benchmark.snappy

import java.io.File

import com.typesafe.config.Config
import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}
import spark.jobserver.{SparkJobInvalid, SparkJobValid, SparkJobValidation}

import org.apache.spark.sql.SnappySQLJob

/**
  * Created by kishor on 28/1/16.
  */
object TPCH_Snappy_Tables extends SnappySQLJob{

   var tpchDataPath: String = _
   var buckets_Order_Lineitem: String = _
   var buckets_Cust_Part_PartSupp: String = _
  var useIndex: Boolean = _
   override def runJob(snc: C, jobConfig: Config): Any = {
     val props : Map[String, String] = null
     val isSnappy = true


     val usingOptionString = s"""
           USING row
           OPTIONS ()"""

     TPCHColumnPartitionedTable.createAndPopulateOrderTable(props, snc, tpchDataPath, isSnappy, buckets_Order_Lineitem)
     TPCHColumnPartitionedTable.createAndPopulateLineItemTable(props, snc, tpchDataPath, isSnappy, buckets_Order_Lineitem)
     TPCHReplicatedTable.createPopulateRegionTable(usingOptionString, props, snc, tpchDataPath, isSnappy)
     TPCHReplicatedTable.createPopulateNationTable(usingOptionString, props, snc, tpchDataPath, isSnappy)
     TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, props, snc, tpchDataPath, isSnappy)
//     TPCHRowPartitionedTable.createPopulateCustomerTable(usingOptionString, props, snc, tpchDataPath, isSnappy, buckets)
//     TPCHRowPartitionedTable.createPopulatePartTable(usingOptionString, props, snc, tpchDataPath, isSnappy, buckets)
//     TPCHRowPartitionedTable.createPopulatePartSuppTable(usingOptionString, props, snc, tpchDataPath, isSnappy, buckets)
     TPCHColumnPartitionedTable.createPopulateCustomerTable(usingOptionString, props, snc, tpchDataPath, isSnappy, buckets_Cust_Part_PartSupp)
     TPCHColumnPartitionedTable.createPopulatePartTable(usingOptionString, props, snc, tpchDataPath, isSnappy, buckets_Cust_Part_PartSupp)
     TPCHColumnPartitionedTable.createPopulatePartSuppTable(usingOptionString, props, snc, tpchDataPath, isSnappy, buckets_Cust_Part_PartSupp)
     if (useIndex) {
       TPCHColumnPartitionedTable.createAndPopulateOrder_CustTable(props, snc, tpchDataPath, isSnappy, buckets_Cust_Part_PartSupp)
       TPCHColumnPartitionedTable.createAndPopulateLineItem_partTable(props, snc, tpchDataPath, isSnappy, buckets_Cust_Part_PartSupp)
     }
   }

   override def validate(sc: C, config: Config): SparkJobValidation = {

     tpchDataPath = if (config.hasPath("dataLocation")) {
       config.getString("dataLocation")
     } else {
       "/QASNAPPY/TPCH/DATA/1"
     }

     buckets_Order_Lineitem = if (config.hasPath("Buckets_Order_Lineitem")) {
       config.getString("Buckets_Order_Lineitem")
     } else {
       "15"
     }

     buckets_Cust_Part_PartSupp = if (config.hasPath("Buckets_Cust_Part_PartSupp")) {
       config.getString("Buckets_Cust_Part_PartSupp")
     } else {
       "15"
     }

     if (!(new File(tpchDataPath)).exists()) {
       return new SparkJobInvalid("Incorrect tpch data path. " +
           "Specify correct location")
     }

     useIndex = if (config.hasPath("useIndex")) {
       config.getBoolean("useIndex")
     } else {
       return new SparkJobInvalid("Specify whether to use Index")
     }
     SparkJobValid
   }
 }
