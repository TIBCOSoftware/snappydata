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
package org.apache.spark.jdbc

import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.core.Data

import org.apache.spark.TaskContext
import org.apache.spark.sql.SnappyContext


class ConnectionConfDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  def testSimpleConnection(): Unit = {
    val snc = SnappyContext(sc)
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.sql("create schema MY_SCHEMA")
    dataDF.write.format("row").saveAsTable("MY_SCHEMA.MY_TABLE")

    val conf = new ConnectionConfBuilder(snc.snappySession).build

    rdd.foreachPartition(d => {
      val conn = ConnectionUtil.getConnection(conf)
      TaskContext.get().addTaskCompletionListener(_ => conn.close())
      val stmt = conn.prepareStatement("update MY_SCHEMA.MY_TABLE set col1 = 9")
      stmt.executeUpdate()
    })

    val result = snc.sql("SELECT col1 FROM MY_SCHEMA.MY_TABLE" )
    result.collect().foreach(v => assert(v(0) == 9))

    snc.sql("drop table MY_SCHEMA.MY_TABLE" )
    snc.sql("drop schema my_schema")

    println("Successful")
  }


}
