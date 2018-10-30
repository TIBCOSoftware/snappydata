/*
 *
 */
package io.snappydata.v2.connector.dummy.snappystore

import java.sql.DriverManager

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class SnappyDataSourceWriter(options: DataSourceOptions, mode: SaveMode, schema: StructType)
    extends DataSourceWriter {
  private val url = options.get("url").orElse("")
  private val tableName = options.get("tableName").orElse("")
  override def createWriterFactory(): DataWriterFactory[Row] =
    new SnappyDataWriterFactory(url, tableName, mode, schema)

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // todo: handle failures here
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}
}

class SnappyDataWriterFactory(url: String, tableName: String, mode: SaveMode, schema: StructType)
    extends DataWriterFactory[Row] {
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new SnappyDataWriter(url, tableName, mode, schema)
  }
}

class SnappyDataWriter(url: String, tableName: String, mode: SaveMode, schema: StructType)
    extends DataWriter[Row] {

  private val conn = DriverManager.getConnection(url)
  private val stmt = conn.createStatement()

  override def write(record: Row): Unit = {
    // assuming hardcoded schema of single numeric column
    stmt.addBatch(s"INSERT INTO $tableName values(${record(0)})")
  }

  override def abort(): Unit = {
    // todo: handle failures here
  }

  override def commit(): WriterCommitMessage = {
    try {
      stmt.executeBatch()
    } finally {
      stmt.close()
      conn.close()
    }
    new SnappyWriteCommitMessage
  }
}

class SnappyWriteCommitMessage() extends WriterCommitMessage