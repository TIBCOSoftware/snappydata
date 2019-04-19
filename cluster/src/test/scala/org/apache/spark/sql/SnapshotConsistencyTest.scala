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
package org.apache.spark.sql

import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.SnappyFunSuite
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.Logging

class SnapshotConsistencyTest
    extends SnappyFunSuite
        with Logging
        with BeforeAndAfter
        with BeforeAndAfterAll {

  val tableName = "T1"

  after {
    snc.sql(s"drop table if exists $tableName")
  }

  test("test insert atomicity in column table") {
    snc.sql(s"DROP TABLE IF EXISTS $tableName")
    snc.sql(s"CREATE TABLE $tableName (Col1 INT not null, Col2 INT not null) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Col1'," +
        "COLUMN_BATCH_SIZE '20000'," +
        "COLUMN_MAX_DELTA_ROWS '50'," +
        "BUCKETS '1')")

    for(i <- 1 to 200) {
      snc.sql(s"insert into $tableName values ($i, $i*10)")
    }

    snc.sql(s"insert into table $tableName select id, id*10 from range(1,101)")
    Misc.getGemFireCache.runOldEntriesCleanerThread()

    val signal = Array(true)

    val r = new Runnable() {
      override def run(): Unit = {
        try {
          for (_ <- 2 to 500) {
            snc.sql(s"insert into table $tableName select id, id*10 from range(1,101)")
          }
        }
        finally {
          this.synchronized {
            signal(0) = false
          }
        }
      }
    }
    val t = new Thread(r)
    t.start()

    while (signal(0)) {
      this.synchronized {
        if (signal(0)) {
          val count = snc.sql(s"select count(col2) from $tableName where col2 > 0").
              collect()(0)(0).asInstanceOf[Long]

          // scalastyle:off
          // println(s"Insert: Table count $count")
          // scalastyle:on
          assert(count % 100 == 0)
        }
      }
      Thread.sleep(5)
    }
    t.join()

    logInfo("Successful")
  }

  test("test delete atomicity in column table") {
    snc.sql(s"DROP TABLE IF EXISTS $tableName")
    snc.sql(s"CREATE TABLE $tableName (Col1 INT not null, Col2 INT not null) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Col1'," +
        "COLUMN_MAX_DELTA_ROWS '5000'," +
        "BUCKETS '1')")

    for (i <- 1 to 200) {
      snc.sql(s"insert into $tableName values ($i, $i*10)")
    }

    snc.sql(s"insert into table $tableName select id, id*10 from range(201,100001)")

    Misc.getGemFireCache.runOldEntriesCleanerThread()
    val signal = Array(true)
    val r = new Runnable() {
      override def run(): Unit = {
        try {
          for (i <- 1 to 80) {
            snc.sql(s"delete from $tableName where col1 <= $i*1000")
            // println(s"SKSK $i th delete")
          }
        }
        finally {
          this.synchronized {
            signal(0) = false
          }
        }
      }
    }

    val t = new Thread(r)
    t.start()

    while (signal(0)) {
      this.synchronized {
        if (signal(0)) {
          val count = snc.sql(s"select count(col2) from $tableName " +
              s"where col2 > 0").collect()(0)(0).asInstanceOf[Long]
          // scalastyle:off
          // println(s"Delete: Table count $count")
          // scalastyle:on
          assert(count % 1000 == 0)
        }
      }
      Thread.sleep(5)
    }

    t.join()
    logInfo("Successful")
  }

  test("test update atomicity in column table") {
    snc.sql(s"DROP TABLE IF EXISTS $tableName")
    snc.sql(s"CREATE TABLE $tableName (Col1 INT not null, Col2 INT not null) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Col1'," +
        "COLUMN_BATCH_SIZE '2000'," +
        "COLUMN_MAX_DELTA_ROWS '5'," +
        "BUCKETS '1')")

    for(i <- 1 to 239) {
      snc.sql(s"insert into $tableName values ($i, $i*10)")
    }

    snc.sql(s"update $tableName set col2 = 1 where col1 > 0")
    Misc.getGemFireCache.runOldEntriesCleanerThread()
    val signal = Array(true)

    val r = new Runnable() {
      override def run(): Unit = {
        try {
          for (i <- 2 to 50) {
            snc.sql(s"update $tableName set col2 = $i where col1 > 0")
            // Misc.getGemFireCache.runOldEntriesCleanerThread()
          }
        }
        finally {
          this.synchronized {
            signal(0) = false
          }
        }
      }
    }
    val t = new Thread(r)
    t.start()

    while (signal(0)) {
      this.synchronized {
        if (signal(0)) {
          // scalastyle:off
          val result = snc.sql(s"select count(col1), sum(col2)" +
              s" from $tableName where col2 > 0").collect()(0)
          // scalastyle:off
          val count = result(0).asInstanceOf[Long]
          val sum = result(1).asInstanceOf[Long]
          // println(s"Update: count: $count and sum $sum")

          // scalastyle:on
          assert(count == 239)
          assert(sum % 239 == 0)
          // scalastyle:on
        }
      }
      Thread.sleep(5)
    }
    t.join()

    logInfo("Successful")
  }

  test("test update atomicity in column table with eviction") {
    snc.sql(s"DROP TABLE IF EXISTS $tableName")
    snc.sql(s"CREATE TABLE $tableName " +
        s"(Col1 INT not null, Col2 INT not null, Col3 String) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Col1'," +
        "COLUMN_BATCH_SIZE '1m'," +
        "COLUMN_MAX_DELTA_ROWS '2000'," +
        "EVICTION_BY 'LRUMEMSIZE 10'," +
        "BUCKETS '1')")

    snc.sql(s"insert into $tableName select id, id*10," +
        s" repeat(id || 'adfasdfasdadsfasd', 100) from range(1,71258)")

    snc.sql(s"update $tableName set col2 = 1 where col1 > 0")

    Misc.getGemFireCache.runOldEntriesCleanerThread()

    val signal = Array(true)
    val r = new Runnable() {
      override def run(): Unit = {
        try {
          for (i <- 2 to 80) {
            snc.sql(s"update $tableName set col2 = $i where col1 > 0")
            Misc.getGemFireCache.runOldEntriesCleanerThread()
            // scalastyle:off
            // println(snc.sql(s"select avg(col2) from $tableName").collect()(0))
            // scalastyle:on
          }
        }
        finally {
          this.synchronized {
            signal(0) = false
          }
        }
      }
    }
    val t = new Thread(r)
    t.start()

    while (signal(0)) {
      this.synchronized {
        if (signal(0)) {
          // scalastyle:off
          val result = snc.sql(s"select count(col1), sum(col2)" +
              s" from $tableName where col2 > 0").collect()(0)
          // scalastyle:off
          val count = result(0).asInstanceOf[Long]
          val sum = result(1).asInstanceOf[Long]
          // println(s"Update with Eviction: count: $count and sum $sum")

          // scalastyle:on
          assert(count == 71257)
          assert(sum % 71257 == 0)

          // scalastyle:on
        }
      }
    }
    t.join()

    logInfo("Successful")
  }


  test("test update atomicity in column table row buffer") {
    snc.sql(s"DROP TABLE IF EXISTS $tableName")
    snc.sql(s"CREATE TABLE $tableName(Col1 INT ,Col2 INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Col1'," +
        "COLUMN_MAX_DELTA_ROWS '5000'," +
        "BUCKETS '1')")

    for (i <- 1 to 1109) {
      snc.sql(s"insert into $tableName values ($i, $i*10)")
    }

    snc.sql(s"update $tableName set col2 = 1 where col1 > 0")

    Misc.getGemFireCache.runOldEntriesCleanerThread()
    val signal = Array(true)
    val r = new Runnable() {
      override def run(): Unit = {
        try {
          for (i <- 2 to 200) {
            snc.sql(s"update $tableName set col2 = $i where col1 > 0")
          }
        }
        finally {
          this.synchronized {
            signal(0) = false
          }
        }
      }
    }
    val t = new Thread(r)
    t.start()

    while (signal(0)) {
      this.synchronized {
        if (signal(0)) {
          val result = snc.sql(s"select count(col1), sum(col2) from" +
              s" $tableName where col1 > 0").collect()(0)
          val count = result(0).asInstanceOf[Long]
          val sum = result(1).asInstanceOf[Long]
          // scalastyle:off
          // println(s"Update in row buffer: count: $count and sum $sum")
          // scalastyle:on

          assert(count == 1109)
          assert(sum % 1109 == 0)

        }
      }
    }

    t.join()

    logInfo("Successful")
  }
}
