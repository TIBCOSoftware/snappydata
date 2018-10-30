/*
 *
 */
package io.snappydata.v2.connector.dummy.snappystore

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader, SupportsPushDownFilters, SupportsReportPartitioning}
import org.apache.spark.sql.sources.v2.reader.partitioning.Partitioning
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class SnappyDataSourceReader(tableName: String, url: String, driver: String,
    size: String) extends DataSourceReader with SupportsPushDownFilters {

  // Schema : We have hardcoded the schema here with single column value.
  def readSchema(): StructType = StructType(Array(StructField("YEAR_", StringType)))

  // Single Factory assuming single Partition
  def createDataReaderFactories: java.util.List[DataReaderFactory[Row]] = {

    val factoryList = new java.util.ArrayList[DataReaderFactory[Row]]
    factoryList.add(
      new SnappyDataSourceReaderFactory(
        tableName, url, driver, size, pushedFilters))
    factoryList
  }

  var pushedFilters: Array[Filter] = Array[Filter]()

  def pushFilters(filters: Array[Filter]): Array[Filter] = {
    pushedFilters = filters
    pushedFilters
  }

}


class SnappyDataSourceReaderFactory(tableName: String, url: String,
    driver: String,
    size: String,
    pushedFilters: Array[Filter])
    extends DataReaderFactory[Row] with DataReader[Row] {

  var stmt: Statement = null
  var conn: Connection = null
  var resultSet: ResultSet = null

  override def createDataReader(): DataReader[Row]
  = new SnappyDataSourceReaderFactory(tableName, url, driver, size, pushedFilters)

  def next: Boolean = {
    if (resultSet == null) {
      print("\n Initializing the result set \n")
      // scalastyle:off
      Class.forName(driver)
      conn = DriverManager.getConnection(url)
      stmt = conn.createStatement()
      resultSet = stmt.executeQuery("SELECT * FROM " + tableName + " LIMIT 20")
    }
    resultSet.next()
  }

  def get: Row = {
    val year = resultSet.getString("YEAR_")
    println(year)
    val row = Row(year)
    row
  }

  def close(): Unit = {
    stmt.close()
    conn.close()
  }

}
