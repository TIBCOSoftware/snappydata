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

package io.snappydata.cluster

import java.sql.DriverManager

import io.snappydata.cluster.TableType.TableType
import io.snappydata.{Constant, StoreTableSizeProvider, StoreTableValueSizeProviderService}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation

class SnappyStoreTableSizeProviderDUnitTest(s: String)
    extends ClusterManagerTestBase(s) {
  val tableName = "APP.TESTTABLE"
  val serviceInterval = "10"
  bootProps.put("spark.snappy.calcTableSizeInterval", serviceInterval)
  bootProps.put("spark.sql.inMemoryColumnarStorage.batchSize", "10")

  def testPartitionedRowTableSize: Unit = {
    val snc = SnappyContext()
    snc.dropTable(tableName, ifExists = true)
    val dataDF = getDF(snc)
    snc.sql(s"CREATE TABLE $tableName (a INT, b INT, c INT) " +
        "USING row  OPTIONS (PARTITION_BY 'a')")

    dataDF.write.insertInto(s"$tableName")

    val result = snc.sql(s"SELECT * FROM $tableName")
    assert(result.collect().length == 20)

    val size = StoreTableSizeProvider.getTableSizes

    val rowDetails = queryMemoryAnalytics(tableName, TableType.ROW)

    assert(size._1.head.rowBufferSize == rowDetails._2)
    assert(size._1.head.rowBufferCount == rowDetails._3)

    snc.sql(s"drop table $tableName")
  }


  def testReplicatedRowTableSize: Unit = {
    val snc = SnappyContext()
    snc.dropTable(tableName, ifExists = true)
    val dataDF = getDF(snc)

    val par = new HashPartitioner(2)


    snc.sql(s"CREATE TABLE $tableName (a INT, b INT, c INT) " +
        "USING row")

    dataDF.write.insertInto(s"$tableName")

    val result = snc.sql(s"SELECT * FROM $tableName")
    assert(result.collect().length == 20)

    val size = StoreTableSizeProvider.getTableSizes

    val rowDetails = queryMemoryAnalytics(tableName, TableType.REPLICATE)

    assert(size._1.head.rowBufferSize == rowDetails._2)
    assert(size._1.head.rowBufferCount == rowDetails._3)

    snc.sql(s"drop table $tableName")
  }


  def testColumnTableSize: Unit = {
    val snc = SnappyContext()
    snc.dropTable(tableName, ifExists = true)
    val dataDF = getDF(snc)

    val par = new HashPartitioner(2)


    snc.sql(s"CREATE TABLE $tableName (a INT, b INT, c INT) " +
        "USING column options (buckets '10')")


    for (i <- 1 to 100) {
      snc.sql(s"insert into $tableName values ($i ,2 , 3)")
    }

    val result = snc.sql(s"SELECT * FROM $tableName")
    assert(result.collect().length == 100)


    val size = StoreTableSizeProvider.getTableSizes

    val rowDetails = queryMemoryAnalytics(tableName, TableType.COLUMN)

    assert(size._2.head.rowBufferSize + size._2.head.columnBufferSize == rowDetails._2)
    assert(size._2.head.rowBufferCount + size._2.head.columnBufferCount == rowDetails._3)


    snc.sql(s"drop table $tableName")
  }

  def testPartitionedTableSizeForQueryOptimization: Unit = {

    def getTableSize: Long =
      StoreTableValueSizeProviderService.getTableSize(tableName).getOrElse(0)

    val snc = SnappyContext()
    snc.dropTable(tableName, ifExists = true)
    snc.sql(s"CREATE TABLE $tableName (a INT, b INT, c INT) " +
        "USING row  OPTIONS (PARTITION_BY 'a' , buckets '3')")

    for (i <- 1 to 100) {
      snc.sql(s"insert into $tableName values ($i ,2 , 3)")
    }

    val result = snc.sql(s"SELECT * FROM $tableName")
    assert(result.collect().length == 100)

    val ma = queryMemoryAnalytics(tableName, TableType.COLUMN)


    ClusterManagerTestBase.waitForCriterion({
      getTableSize == 2800
    },
      s"Comparing StoreTableValueSizeProviderService partitioned table size "
          + getTableSize + " with expected size 2800",
      serviceInterval.toInt * 5, serviceInterval.toInt, throwOnTimeout = true)

    snc.dropTable(tableName)
  }


  def testColumnTableSizeForQueryOptimization: Unit = {

    def getTableSize(table: String): Long =
      StoreTableValueSizeProviderService.getTableSize(table).getOrElse(0)

    val snc = SnappyContext()
    val dataDF = getDF(snc)

    snc.sql(s"CREATE TABLE $tableName (a INT, b INT, c INT) " +
        "USING column  options (buckets '3')")

    for (i <- 1 to 100) {
      snc.sql(s"insert into $tableName values ($i ,2 , 3)")
    }

    val result = snc.sql(s"SELECT * FROM $tableName")
    assert(result.collect().length == 100)

    val colBuffer = ColumnFormatRelation.cachedBatchTableName(tableName)

    val ma = queryMemoryAnalytics(tableName, TableType.COLUMN)
    ClusterManagerTestBase.waitForCriterion({
      (getTableSize(colBuffer) == 1548 && getTableSize(tableName) == 360)
    },
      s"Comparing StoreTableValueSizeProviderService Column table size (ColumnBuffer:RowBuffer) "
          + getTableSize(colBuffer) + ":" + getTableSize(tableName) + " with expected (1548:360)",
      serviceInterval.toInt * 5, serviceInterval.toInt, throwOnTimeout = true)

    snc.dropTable(tableName)

  }

  private def queryMemoryAnalytics(tableName: String, tableType: TableType):
  (String, Long, Int, Long) = {

    val query = "SELECT  SUM(TOTAL_SIZE)," +
        "  SUM(NUM_ROWS)," +
        "  sum(value_size)," +
        "  COUNT(HOST)" +
        " FROM SYS.MEMORYANALYTICS" +
        s" WHERE TABLE_NAME = '$tableName' " +
        s" group by table_name"

    var totalSize: Long = 0
    var totalRows: Int = 0
    var valueSize: Long = 0

    val conn = DriverManager.getConnection(Constant.DEFAULT_EMBEDDED_URL)
    val rs = conn.createStatement().executeQuery(query)
    if (rs.next()) {

      tableType match {
        case TableType.COLUMN => {
          val columnBufferName = ColumnFormatRelation.cachedBatchTableName(tableName)
          val colDetails = queryMemoryAnalytics(columnBufferName, TableType.ROW)
          totalSize = (rs.getString(1).toDouble * 1024).toLong + colDetails._2
          totalRows = rs.getString(2).toInt + colDetails._3
          valueSize = (rs.getString(3).toDouble * 1024).toLong + colDetails._4
        }
        case TableType.REPLICATE => {
          val host = rs.getInt(4)
          totalSize = (rs.getString(1).toDouble * 1024).toLong / host
          totalRows = rs.getString(2).toInt / host
          valueSize = (rs.getString(3).toDouble * 1024).toLong / host
        }
        case TableType.ROW => {
          totalSize = (rs.getString(1).toDouble * 1024).toLong
          totalRows = rs.getString(2).toInt
          valueSize = (rs.getString(3).toDouble * 1024).toLong
        }
      }
    }

    conn.close()

    (tableName, totalSize, totalRows, valueSize)
  }


  private def getDF(snc: SnappyContext) = {
    val data = for (i <- 1 to 20) yield Seq(i, 2, 3)
    val rdd = sc.parallelize(data, data.length).
        map(s => new io.snappydata.core.Data(s.head, s(1), s(2)))
    snc.createDataFrame(rdd)
  }

}


object TableType extends Enumeration {
  type TableType = Value
  val ROW, COLUMN, REPLICATE = Value
}