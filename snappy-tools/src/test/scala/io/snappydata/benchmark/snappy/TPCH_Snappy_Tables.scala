package io.snappydata.benchmark.snappy

import java.io.File

import com.typesafe.config.Config
import io.snappydata.benchmark.{TPCHReplicatedTable, TPCHRowPartitionedTable, TPCHColumnPartitionedTable}
import spark.jobserver.{SparkJobInvalid, SparkJobValid, SparkJobValidation}

import org.apache.spark.sql.SnappySQLJob

/**
  * Created by kishor on 28/1/16.
  */
object TPCH_Snappy_Tables extends SnappySQLJob{

   var tpchDataPath: String = _
   var buckets: String = _

   override def runJob(snc: C, jobConfig: Config): Any = {
     val props : Map[String, String] = null
     val isSnappy = true


     val usingOptionString = s"""
           USING row
           OPTIONS ()"""

     println("KBKBKB: Bucets : " + buckets)
     TPCHColumnPartitionedTable.createAndPopulateOrderTable(props, snc, tpchDataPath, isSnappy, buckets)
     TPCHColumnPartitionedTable.createAndPopulateLineItemTable(props, snc, tpchDataPath, isSnappy, buckets)
     TPCHRowPartitionedTable.createPopulateCustomerTable(usingOptionString, props, snc, tpchDataPath, isSnappy)
     TPCHReplicatedTable.createPopulateRegionTable(usingOptionString, props, snc, tpchDataPath, isSnappy)
     TPCHReplicatedTable.createPopulateNationTable(usingOptionString, props, snc, tpchDataPath, isSnappy)
     TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, props, snc, tpchDataPath, isSnappy)
     TPCHRowPartitionedTable.createPopulatePartTable(usingOptionString, props, snc, tpchDataPath, isSnappy)
     TPCHRowPartitionedTable.createPopulatePartSuppTable(usingOptionString, props, snc, tpchDataPath, isSnappy)

   }

   override def validate(sc: C, config: Config): SparkJobValidation = {

     tpchDataPath = if (config.hasPath("dataLocation")) {
       config.getString("dataLocation")
     } else {
       "/QASNAPPY/TPCH/DATA/1"
     }

     buckets = if (config.hasPath("Buckets")) {
       config.getString("Buckets")
     } else {
       "15"
     }

     if (!(new File(tpchDataPath)).exists()) {
       return new SparkJobInvalid("Incorrect tpch data path. " +
           "Specify correct location")
     }


     SparkJobValid
   }
 }
