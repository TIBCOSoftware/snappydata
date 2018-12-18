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

import java.sql.{SQLException, DriverManager}

import io.snappydata.SnappyFunSuite
import io.snappydata.core.Data
import org.apache.spark.{TaskContext, Logging}
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.SaveMode


class ConnectionConfTest extends SnappyFunSuite with Logging with BeforeAndAfter {

  test("test default conf") {
    val conf = new ConnectionConfBuilder(snc.snappySession).build()
    assert(!conf.connProps.hikariCP)

    val conn = ConnectionUtil.getPooledConnection("test default conf", conf)
    assert(conn.getSchema != null)
    conn.commit()
    conn.close()
  }

  test("test tomcat conf") {
    val conf = new ConnectionConfBuilder(snc.snappySession).setPoolProvider("tomcat").build()
    assert(!conf.connProps.hikariCP)

    val conn = ConnectionUtil.getPooledConnection("test default conf", conf)
    assert(conn.getSchema != null)
    conn.commit()
    conn.close()
  }

  test("test Additional hikari conf") {
    val conf = new ConnectionConfBuilder(snc.snappySession).setPoolProvider("hikari")
        .setPoolConf("maximumPoolSize", "50").build()
    assert(conf.connProps.hikariCP)
    assert(conf.connProps.poolProps("maximumPoolSize") == "50")

    val conn = ConnectionUtil.getPooledConnection("test default conf", conf)
    assert(conn.getSchema != null)
    conn.commit()
    conn.close()
  }

  test("test multiple hikari conf") {
    val conf = new ConnectionConfBuilder(snc.snappySession).setPoolProvider("hikari")
        .setPoolConf("maximumPoolSize", "50")
        .setPoolConf("minimumIdle", "5")
        .build()
    assert(conf.connProps.hikariCP)
    assert(conf.connProps.poolProps("maximumPoolSize") == "50")
    assert(conf.connProps.poolProps("minimumIdle") == "5")

    val conn = ConnectionUtil.getPooledConnection("test default conf", conf)
    assert(conn.getSchema != null)
    conn.commit()
    conn.close()
  }

  test("test multiple hikari conf by map") {
    val poolProps = Map("maximumPoolSize" -> "50", "minimumIdle" -> "5")
    val conf = new ConnectionConfBuilder(snc.snappySession).setPoolProvider("hikari")
        .setPoolConfs(poolProps)
        .build()
    assert(conf.connProps.hikariCP)
    assert(conf.connProps.poolProps("maximumPoolSize") == "50")
    assert(conf.connProps.poolProps("minimumIdle") == "5")

    val conn = ConnectionUtil.getPooledConnection("test default conf", conf)
    assert(conn.getSchema != null)
    conn.commit()
    conn.close()
  }

  test("test multiple tomcat conf by map") {
    val poolProps = Map("maxActive" -> "50", "maxIdle" -> "80", "initialSize" -> "5")
    val conf = new ConnectionConfBuilder(snc.snappySession).setPoolProvider("tomcat")
        .setPoolConfs(poolProps)
        .build()
    assert(!conf.connProps.hikariCP)
    assert(conf.connProps.poolProps("maxActive") == "50")
    assert(conf.connProps.poolProps("maxIdle") == "80")
    assert(conf.connProps.poolProps("initialSize") == "5")

    val conn = ConnectionUtil.getPooledConnection("test default conf", conf)
    assert(conn.getSchema != null)
    conn.commit()
    conn.close()
  }

  test("test serializibility") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => Data(s.head, s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.sql("create schema my_schema")
    dataDF.write.format("row").saveAsTable("MY_SCHEMA.MY_TABLE")

    val conf = new ConnectionConfBuilder(snc.snappySession).build()

    rdd.foreachPartition(d => {
      val conn = ConnectionUtil.getPooledConnection("test", conf)
      TaskContext.get().addTaskCompletionListener(_ => {
        conn.commit()
        conn.close()
      })
      val stmt = conn.prepareStatement("update MY_SCHEMA.MY_TABLE set col1 = 9")
      stmt.executeUpdate()
    })

    val result = snc.sql("SELECT col1 FROM MY_SCHEMA.MY_TABLE")
    result.collect().foreach(v => assert(v(0) == 9))

    snc.sql("drop table MY_SCHEMA.MY_TABLE")
    snc.sql("drop schema my_schema")

    logInfo("Successful")
  }

  test("test a simple connection") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => Data(s.head, s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.sql("create schema my_schema")
    dataDF.write.format("row").saveAsTable("MY_SCHEMA.MY_TABLE")

    val conf = new ConnectionConfBuilder(snc.snappySession).build

    rdd.foreachPartition(d => {
      val conn = ConnectionUtil.getConnection(conf)
      TaskContext.get().addTaskCompletionListener(_ => {
        conn.commit()
        conn.close()
      })
      val stmt = conn.prepareStatement("update MY_SCHEMA.MY_TABLE set col1 = 9")
      stmt.executeUpdate()
    })

    val result = snc.sql("SELECT col1 FROM MY_SCHEMA.MY_TABLE")
    result.collect().foreach(v => assert(v(0) == 9))

    snc.sql("drop table MY_SCHEMA.MY_TABLE")
    snc.sql("drop schema my_schema")

    logInfo("Successful")
  }

  val path = "ConnectionConfTest"
  test("test non-snappy data source") {

    val props = Map(
      "url" -> s"jdbc:derby:$path",
      "driver" -> "org.apache.derby.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )

    // Start the database
    val conn = DriverManager.getConnection(s"jdbc:derby:$path;create=true")
    conn.createStatement().execute(
      "create table TEST_JDBC_TABLE_1(COL1 INTEGER,COL2 INTEGER,COL3 INTEGER)")

    snc.sql("DROP TABLE IF EXISTS TEST_JDBC_TABLE_1")
    snc.sql(s"CREATE external TABLE TEST_JDBC_TABLE_1 USING jdbc " +
      s"options(url 'jdbc:derby:$path',driver 'org.apache.derby.jdbc.EmbeddedDriver'," +
        "dbtable 'TEST_JDBC_TABLE_1', poolImpl 'tomcat',user 'app',password 'app')")

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => Data(s.head, s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("jdbc").mode(SaveMode.Overwrite).options(props)
      .insertInto("TEST_JDBC_TABLE_1")
    val connConf = new ConnectionConfBuilder(snc.snappySession)
    props.map(entry => connConf.setConf(entry._1, entry._2))
    val conf = connConf.build()

    try {
      rdd.foreachPartition(d => {
        val conn = ConnectionUtil.getPooledConnection("testDerby", conf)
        TaskContext.get().addTaskCompletionListener(_ => {
          conn.commit()
          conn.close()
        })
        val stmt = conn.prepareStatement("update TEST_JDBC_TABLE_1 set col1 = 9")
        stmt.executeUpdate()
      })

      val result = snc.sql("SELECT col1 from TEST_JDBC_TABLE_1")
      result.show()
      // result.collect().foreach(v => assert(v(0) == 9))

      snc.sql("drop table TEST_JDBC_TABLE_1")
    } finally {
      shutdownDerbyDatabase()
    }
  }

  private def shutdownDerbyDatabase(): Unit = {
    try {
      DriverManager.getConnection(s"jdbc:derby:$path;shutdown=true")
    } catch {
      // Throw if not normal single database shutdown
      // https://db.apache.org/derby/docs/10.2/ref/rrefexcept71493.html
      case sqlEx: SQLException =>
        if (sqlEx.getSQLState != "08006" && sqlEx.getSQLState != "XJ015") {
          throw sqlEx
        }
    } finally {
    }
  }
}
