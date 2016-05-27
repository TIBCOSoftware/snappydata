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

import java.io.PrintWriter

import com.typesafe.config.Config
import spark.jobserver.{SparkJobValid, SparkJobValidation}

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.SnappyStreamingJob
import org.apache.spark.sql.types.{IntegerType, StructType, TimestampType}
import org.apache.spark.streaming.Duration

class BenchmarkingStreamingJob extends SnappyStreamingJob {

  override def runJob(snsc: C, jobConfig: Config): Any = {
    val st = s"StartJob-${System.currentTimeMillis()}.out"
    // scalastyle:off println

    val pw_st = new PrintWriter(st)
    pw_st.println(jobConfig.getInt("maxRecPerSecond"))
    pw_st.println(jobConfig.getInt("numWarehouses"))
    pw_st.println(jobConfig.getInt("numDistrictsPerWarehouse"))
    pw_st.println(jobConfig.getInt("numCustomersPerDistrict"))
    pw_st.println(jobConfig.getInt("numItems"))
    pw_st.close()
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
      .add("cs_timespent", IntegerType)
      .add("cs_click_d", TimestampType)

    val rows = stream.map(v => Row(v.w_id,
      v.d_id, v.c_id, v.i_id, v.c_ts, new java.sql.Timestamp(System.currentTimeMillis)))

    val window_rows = rows.window(new Duration(60*1000), new Duration(60*1000))

    val windowStreamAsTable = snsc.createSchemaDStream(window_rows, schema)

    snsc.sql("set spark.sql.shuffle.partitions="+ jobConfig.getInt("shufflePartitions"))
    windowStreamAsTable.foreachDataFrame(df =>
      {
        val outFileName = s"BenchmarkingStreamingJob-${System.currentTimeMillis()}.out"

        val pw = new PrintWriter(outFileName)
        // Find out top items in this window

        val clickstreamlog = "benchmarking" + System.currentTimeMillis()
        df.registerTempTable(clickstreamlog)
//        val resultdfQ1 = snsc.sql(s"Select cs_i_id, count(cs_i_id) as cnt " +
//          s"From oorder_col, order_line_col, $clickstreamlog " +
//          s"Where ol_i_id = cs_i_id " +
//          s"And ol_w_id = o_w_id" +
//          s" And ol_d_id = o_d_id " +
//          s"And ol_o_id = o_id " +
//          s"Group by cs_i_id " +
//          s"Order by cnt " )

        // Find out the items in the clickstream with
        // price range greater than a particular amount.
        val resultdfQ1 = snsc.sql(s"select i_id, count(i_id) from " +
          s" $clickstreamlog, item " +
          s" where i_id = cs_i_id " +
          s" AND i_price > 50 " +
          s" GROUP BY i_id ");

        // Find out which district's customer are currently more online active to
        // stop tv commercials in those districts
        val resultdfQ2 = snsc.sql(s" select avg(cs_timespent) as avgtimespent , cs_c_d_id " +
          s"from $clickstreamlog  group by cs_c_d_id order by avgtimespent")

        val output = if (jobConfig.getBoolean("printResults")){
          val sq1 = System.currentTimeMillis()
          resultdfQ1.limit(10).collect().foreach(pw.println)
          val endq1 = System.currentTimeMillis()
          resultdfQ2.collect().foreach(pw.println)
          val endq2 = System.currentTimeMillis()
          s"Q1 ${endq1 - sq1} Q2 ${endq2 -endq1}"
        } else {
          val sq1 = System.currentTimeMillis()
          resultdfQ1.limit(10).collect()
          val endq1 = System.currentTimeMillis()
          resultdfQ2.collect()
          val endq2 = System.currentTimeMillis()
          s"Q1 ${endq1 - sq1} Q2 ${endq2 -endq1}"
        }
        pw.println(s"Time taken $output")
        pw.close()
      })


    snsc.start()
    Thread.sleep(jobConfig.getInt("streamTime"))
    snsc.stop(false)

  }

  override def validate(snsc: C, config: Config): SparkJobValidation = {
    SparkJobValid
  }


}
