/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

package io.snappydata.examples

import com.typesafe.config.Config

import org.apache.spark.TaskContext
import org.apache.spark.jdbc.{ConnectionUtil, ConnectionConfBuilder}
import org.apache.spark.sql.{SnappySession, SnappyJobValid, SaveMode, SnappyJobValidation, SnappyContext, SnappySQLJob}

case class Data(col1: Int, col2: Int, col3: Int)

/**
 * An example to use ConnectionUtil to mutate data stored in SnappyData.
 * 
 */
object DataUpdateJob extends SnappySQLJob {
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snc: SnappySession, jobConfig: Config): Any = {
    val sc = snc.sparkContext
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    val tableName = "MY_SCHEMA.MY_TABLE"

    dataDF.write.format("row").mode(SaveMode.Append).saveAsTable(tableName)

    val conf = new ConnectionConfBuilder(snc).build

    rdd.foreachPartition(d => {
      val conn = ConnectionUtil.getConnection(conf)
      TaskContext.get().addTaskCompletionListener(_ => conn.close())
      val stmt = conn.prepareStatement("update MY_SCHEMA.MY_TABLE set col1 = 9")
      stmt.executeUpdate()
    })
  }
}
