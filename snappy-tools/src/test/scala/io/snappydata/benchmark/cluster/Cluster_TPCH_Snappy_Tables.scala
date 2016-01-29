/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package io.snappydata.benchmark.cluster

import com.typesafe.config.Config
import io.snappydata.benchmark.{TPCHReplicatedTable, TPCHRowPartitionedTable, TPCHColumnPartitionedTable}
import spark.jobserver.{SparkJobValid, SparkJobValidation}

import org.apache.spark.sql.SnappySQLJob

/**
  * Created by kishor on 28/1/16.
  */
object Cluster_TPCH_Snappy_Tables extends SnappySQLJob{



   override def runJob(snc: C, jobConfig: Config): Any = {
     val props : Map[String, String] = null
     val isSnappy = true
     val path = "/QASNAPPY/TPCH/DATA/GB10"

     val usingOptionString = s"""
           USING row
           OPTIONS ()"""

     TPCHColumnPartitionedTable.createAndPopulateOrderTable(props, snc, path, isSnappy)
     TPCHColumnPartitionedTable.createAndPopulateLineItemTable(props, snc, path, isSnappy)
     TPCHRowPartitionedTable.createPopulateCustomerTable(usingOptionString, props, snc, path, isSnappy)
     TPCHReplicatedTable.createPopulateRegionTable(usingOptionString, props, snc, path, isSnappy)
     TPCHReplicatedTable.createPopulateNationTable(usingOptionString, props, snc, path, isSnappy)
     TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, props, snc, path, isSnappy)
     TPCHRowPartitionedTable.createPopulatePartTable(usingOptionString, props, snc, path, isSnappy)
     TPCHRowPartitionedTable.createPopulatePartSuppTable(usingOptionString, props, snc, path, isSnappy)

   }

   override def validate(sc: C, config: Config): SparkJobValidation = {
     SparkJobValid
   }
 }
