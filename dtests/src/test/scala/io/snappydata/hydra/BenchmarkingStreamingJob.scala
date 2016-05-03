/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.snappydata.hydra

import com.typesafe.config.Config
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.SnappyStreamingJob
import org.apache.spark.sql.types.{TimestampType, IntegerType, LongType, StructType}
import org.apache.spark.streaming.Duration
import spark.jobserver.{SparkJobValid, SparkJobValidation}

class BenchmarkingStreamingJob extends SnappyStreamingJob {

  override def runJob(snsc: C, jobConfig: Config): Any = {
    val stream = snsc.receiverStream[ClickStreamCustomer](
      new BenchmarkingReceiver(jobConfig.getInt("maxRecPerSecond"),
      jobConfig.getInt("numWarehouses"),
      jobConfig.getInt("numDistrictsPerWarehouse"),
      jobConfig.getInt("numCustomersPerDistrict"),
      jobConfig.getInt("numItems")))


    val schema = new StructType()
      .add("cs_c_w_id", IntegerType)
      .add("cs_c_d_id", IntegerType)
      .add("cs_c_id", IntegerType)
      .add("cs_i_id", IntegerType)
      .add("cs_click_d", TimestampType)


    val rows = stream.map(v => Row(new java.sql.Timestamp(System.currentTimeMillis), v.w_id,
      v.d_id, v.c_id, v.i_id))
    val window_rows = rows.window(new Duration(30*100), new Duration(5*1000))

    val windowStreamAsTable = snsc.createSchemaDStream(window_rows, schema)
    val logStreamAsTable = snsc.createSchemaDStream(rows, schema)

    snsc.snappyContext.createTable("clickstream_col", "column", schema,
      Map("buckets" -> "41"))

    windowStreamAsTable.foreachDataFrame(_.agg( Map( "" -> "")))

    snsc.start()
    Thread.sleep(jobConfig.getInt("streamTime"))
    snsc.stop(false)

  }

  override def validate(snsc: C, config: Config): SparkJobValidation = {
    SparkJobValid
  }


}
