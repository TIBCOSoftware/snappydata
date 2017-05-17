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
package org.apache.spark.sql.streaming.jdbc

import java.sql.{DriverManager, ResultSet}
import java.util.Properties

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{DateType, NumericType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SQLContext}

class JdbcStreamProvider extends StreamSourceProvider with DataSourceRegister {
  @Override
  def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    (parameters("dbtable"), JDBCSource(sqlContext, parameters).schema)
  }

  @Override
  def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    JDBCSource(sqlContext, parameters)
  }

  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   *
   * {{{
   *   override def shortName(): String = "parquet"
   * }}}
   *
   * @since 1.5.0
   */
  override def shortName(): String = "jdbcStream"
}

case class LSN(lsn: String) extends Offset

case class JDBCSource(@transient sqlContext: SQLContext,
    parameters: Map[String, String]) extends Source with Logging {

  private val dbtable = parameters("dbtable")

  private val offsetCol = parameters("jdbc.offsetColumn")

  private val offsetToStrFunc = parameters("jdbc.offsetToStrFunc")

  private val strToOffsetFunc = parameters("jdbc.strToOffsetFunc")

  private val offsetIncFunc = parameters("jdbc.offsetIncFunc")

  private val getNextOffset = parameters.get("jdbc.getNextOffset")

  private val streamStateTable = "SNAPPY_JDBC_STREAM_CONTEXT"

  private val partitioner = parameters.get("partitionBy").fold(
    parameters.get(s"$dbtable.partitionBy"))(Some(_))

  private val partitioningQuery = parameters.get("partitioningQuery").fold(
    parameters.get(s"$dbtable.partitioningQuery"))(Some(_))

  private val reader =
    parameters.foldLeft(sqlContext.sparkSession.read)({
      case (r, (k, v)) => r.option(k, v)
    })

  private val srcControlConn = {
    val connProps = parameters.foldLeft(new Properties()) {
      case (prop, (k, v)) if !k.toLowerCase.startsWith("snappydata.cluster.") =>
        prop.setProperty(k, v)
        prop
      case (p, _) => p
    }
    DriverManager.getConnection(parameters("url"), connProps)
  }

  createContextTableIfNotExist()

  private lazy val stateTableUpdate = srcControlConn.prepareStatement(s"update $streamStateTable " +
      s" set lastOffset = ? where tableName = ?")

  private val lastPersistedOffset: Option[LSN] = {
    tryExecuteQuery("select lastOffset from " +
        s"$streamStateTable where tableName = '$dbtable'") { rs =>
      if (rs.next()) {
        if (parameters.getOrElse("fullMode", "false").toBoolean) {
          val rowsEffected = srcControlConn.createStatement().executeUpdate("delete from " +
              s"$streamStateTable where tableName = '$dbtable'")
          if (rowsEffected != 0) {
            logInfo(s"cleared the last known state for $dbtable recorded offset ${rs.getString(1)}")
          }
          None
        }
        else {
          Some(LSN(rs.getString(1)))
        }
      } else {
        None
      }
    }
  }

  private var lastOffset: Option[LSN] = lastPersistedOffset

  private lazy val cachedPartitions = partitioningQuery match {
    case Some(q) if partitioner.nonEmpty =>
      determinePartitions(
        s"( ${q.replaceAll("\\$partitionBy", partitioner.get)} ) getUniqueValuesForCaching",
        partitioner.get)
    case _ => Array.empty[String]
  }

  /**
   * Returns the maximum available offset for this source.
   * Returns `None` if this source has never received any data.
   */
  override def getOffset: Option[Offset] = {
    def fetch(rs: ResultSet) = {
      if (rs.next() && rs.getString(1) != null) {
        Some(LSN(rs.getString(1)))
      } else {
        None
      }
    }

    lastOffset match {
      case None =>
        val newlsn = tryExecuteQuery(
          s"select $offsetToStrFunc(min($offsetCol)) newOffset from $dbtable")(fetch)
        saveOffsetState(newlsn)
      case Some(offset) =>
        // while picking the nextoffset by the user defined query if no offset gets fetched
        // pick the builtin way of finding the next offset.
        tryExecuteQuery(getNextOffsetQuery())(fetch) match {
          case None =>
            val nextOffset = tryExecuteQuery(getNextOffsetQuery(true))(fetch)
            if(nextOffset.nonEmpty) {
              alertUser(s"No rows found using the user query for getNextOffset," +
                  s" lastoffset -> ${lastOffset.get.lsn}")
            }
            saveOffsetState(nextOffset)
          case o@Some(_) => saveOffsetState(o)
        }
    }

    lastOffset
  }

  @Override
  def getBatch(start: Option[Offset], end: Offset): DataFrame = {

    def lsn(o: Offset) = o.asInstanceOf[LSN].lsn

    def getPartitions(query: String) = partitioner match {
      case _ if cachedPartitions.nonEmpty => cachedPartitions
      case Some(col) =>
        val uniqueValQuery = query.replace("select * from",
          s"select distinct($col) parts from ").replace("getBatch", "getUniqueValuesFromBatch")
        determinePartitions(uniqueValQuery, col)
      case None => Array.empty[String]
    }

    start match {
      case None => lastPersistedOffset match {
        case None =>
          execute(s"(select * from $dbtable " +
              s" where $offsetCol <= $strToOffsetFunc('${lsn(end)}') ) getBatch ", getPartitions)
        case Some(offset) =>
          alertUser(s"Resuming source [${parameters("url")}] [$dbtable] " +
              s"from persistent offset -> $offset ")
          execute(s"(select * from $dbtable " +
              s" where $offsetCol > $strToOffsetFunc('${offset.lsn}') " +
              s" and $offsetCol <= $strToOffsetFunc('${lsn(end)}') ) getBatch ", getPartitions)
      }
      case Some(begin) =>
        execute(s"(select * from $dbtable " +
            s" where $offsetCol > $strToOffsetFunc('${lsn(begin)}') " +
            s" and $offsetCol <= $strToOffsetFunc('${lsn(end)}') ) getBatch ", getPartitions)
    }
  }

  private def determinePartitions(query: String, partCol: String): Array[String] = {
    val uniqueVals = execute(query)
    val partitionField = uniqueVals.schema.fields(0)
    // just one partition so sortWithinPartitions is enough
    val sorted = uniqueVals.sortWithinPartitions(partitionField.name).collect()
    val parallelism = sqlContext.sparkContext.defaultParallelism
    val slide = sorted.length / parallelism
    if (slide > 0) {
      val converter = CatalystTypeConverters.createToScalaConverter(partitionField.dataType)
      sorted.sliding(slide, slide).map { a =>
        partitionField.dataType match {
          case _: NumericType =>
            s"$partCol >= ${a(0)(0)} and $partCol < ${a(a.length - 1)(0)}"
          case _@(StringType | DateType | TimestampType) =>
            s"$partCol >= '${a(0)(0)}' and $partCol < '${a(a.length - 1)(0)}'"
        }
      }.toArray
    } else {
      Array.empty[String]
    }
  }

  private def createContextTableIfNotExist() = {
    val rs = srcControlConn.getMetaData.getTables(null, null, streamStateTable, null)
    if (!rs.next()) {
      logInfo(s"$streamStateTable table not found. creating ..")
      if (!srcControlConn.createStatement().execute(s"create table $streamStateTable(" +
          " tableName varchar(200) primary key," +
          " lastOffset varchar(100)" +
          ")")) {
        logWarning(s"$streamStateTable couldn't be created on the jdbc source $this")
      }
    }
  }

  private def execute(query: String,
      partitions: String => Array[String] = _ => Array.empty) = partitions(query) match {
    case parts if parts.nonEmpty =>
      reader.jdbc(parameters("url"), query, parts, new Properties())
    case _ =>
      reader.jdbc(parameters("url"), query, new Properties())
  }

  private def tryExecuteQuery[T](query: String)(f: ResultSet => T) = {
    val rs = srcControlConn.createStatement().executeQuery(query)
    try {
      f(rs)
    } finally {
      rs.close()
    }
  }


  private def getNextOffsetQuery(forceMin: Boolean = false): String = {
    if (getNextOffset.isEmpty || forceMin) {
      s"select $offsetToStrFunc(min($offsetCol)) nxtOffset from $dbtable " +
          s" where $offsetCol > $offsetIncFunc($strToOffsetFunc('${lastOffset.get.lsn}'))"
    } else {
      getNextOffset.get
          .replaceAll("\\$table", dbtable)
          .replaceAll("\\$currentOffset", lastOffset.get.lsn)
    }
  }

  private def saveOffsetState(newlsn: Option[LSN]) = if (newlsn.nonEmpty) {
    try {
      lastOffset = newlsn
      // update the state table at source.
      stateTableUpdate.setString(1, lastOffset.get.lsn)
      stateTableUpdate.setString(2, dbtable)
      val rowsEffected = stateTableUpdate.executeUpdate()
      if (rowsEffected == 0) {
        val rowsInserted = srcControlConn.createStatement().executeUpdate(s"insert into " +
            s"$streamStateTable values ('$dbtable', '${lastOffset.get.lsn}') ")
        assert(rowsInserted == 1)
      }
    } catch {
      case e: Exception =>
    }
  }

  private def alertUser(msg: => String): Unit = {
    println(msg)
    logInfo(msg)
  }

  /** Returns the schema of the data from this source */
  override def schema: StructType = {
    execute(dbtable).schema
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = {}
}

