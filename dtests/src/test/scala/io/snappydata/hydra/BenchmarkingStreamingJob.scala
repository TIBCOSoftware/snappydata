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
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.SnappyStreamingJob
import org.apache.spark.sql.types.{TimestampType, IntegerType, LongType, StructType}
import org.apache.spark.streaming.Duration
import spark.jobserver.{SparkJobValid, SparkJobValidation}

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
      .add("cs_click_d", TimestampType)

    val rows = stream.map(v => Row(v.w_id,
      v.d_id, v.c_id, v.i_id, new java.sql.Timestamp(System.currentTimeMillis)))

    val window_rows = rows.window(new Duration(30*1000), new Duration(6*1000))

    val windowStreamAsTable = snsc.createSchemaDStream(window_rows, schema)

    snsc.snappyContext.createTable("clickstream_col", "column", schema,
      Map("buckets" -> "41"))

    import org.apache.spark.sql.functions._
    windowStreamAsTable.foreachDataFrame(df =>
      {
        val outFileName = s"BenchmarkingStreamingJob-${System.currentTimeMillis()}.out"

        val pw = new PrintWriter(outFileName)
        // Find out top items in this window
        val start = System.currentTimeMillis()
        df.groupBy(df.col("cs_i_id")).agg(count("cs_i_id").alias("itemcount")).
          orderBy(desc("itemcount")).collect().foreach(pw.println)

        pw.println(s"Time taken ${System.currentTimeMillis() - start}")
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
