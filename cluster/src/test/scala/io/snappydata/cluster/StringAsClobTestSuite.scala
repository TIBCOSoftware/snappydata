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
package io.snappydata.cluster

import java.sql.DriverManager

import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfterAll

class StringAsClobTestSuite extends SnappyFunSuite with BeforeAndAfterAll {

  private val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE
  var serverHostPort = ""
  val tableName = "order_line_col"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // reducing DML chunk size size to force lead node to send
    // results in multiple batches
    setDMLMaxChunkSize(50L)
  }

  override def afterAll(): Unit = {
    setDMLMaxChunkSize(default_chunk_size)
    super.afterAll()
  }

  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }

  test("Test char") {
    snc
    val serverHostPort2 = TestUtil.startNetServer()
    logInfo("network server started")
    val conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
    val s = conn.createStatement()
    s.executeUpdate(s"create table $tableName (id int not null primary key, name String, address " +
        "String) USING row OPTIONS(partition_by 'id')")
    // "String) partition by column(id)")
    s.executeUpdate(s"insert into $tableName values(111, 'aaa', 'hello')")
    s.executeUpdate(s"insert into $tableName values(222, 'bbb', 'halo')")
    s.executeUpdate(s"insert into $tableName values(333, 'aaa', 'hello')")
    s.executeUpdate(s"update $tableName set name='abc1' where id=111")
    val rs = s.executeQuery(s"select id, name, address from $tableName")
    while (rs.next()) {
      logInfo(s"${rs.getInt(1)} ${rs.getString(2)} ${rs.getString(3)}")
    }
    val rs2 = s.executeQuery(s"select id from $tableName where name='abc1'")
    if (rs2.next()) {
      assert(rs2.getInt(1) == 111)
    }

    rs.close()
    rs2.close()
    conn.close()
  }
}
