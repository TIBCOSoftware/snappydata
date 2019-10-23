/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark.sql.streaming

import java.sql.{DriverManager, SQLException}
import java.util.NoSuchElementException

import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState.{LOGIN_FAILED, SNAPPY_CATALOG_SCHEMA_VERSION_MISMATCH}
import io.snappydata.Property._
import io.snappydata.util.ServiceUtils

import org.apache.spark.Logging
import org.apache.spark.sql.execution.CatalogStaleException
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.SnappyStoreSinkProvider.EventType._
import org.apache.spark.sql.streaming.SnappyStoreSinkProvider._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SnappyContext, SnappySession, _}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

/**
 * Should be implemented by clients who wants to override default behavior provided by
 * [[DefaultSnappySinkCallback]].
 * <p>
 * In order to override the default callback behavior the qualified name of the implementing
 * class needs to be passed against `sinkCallback` option while defining stream query.
 *
 */
trait SnappySinkCallback {

  /**
   * This method is called for each streaming batch after checking the possibility of batch
   * duplication which is indicated by `possibleDuplicate` flag.
   * <p>
   * A duplicate batch might be picked up for processing in case of failure. In case of batch
   * duplication, this method should handle batch in idempotent manner in order to avoid
   * data inconsistency.
   */
  def process(snappySession: SnappySession, sinkProps: Map[String, String],
      batchId: Long, df: Dataset[Row], possibleDuplicate: Boolean = false): Unit
}

class SnappyStoreSinkProvider extends StreamSinkProvider with DataSourceRegister {

  @Override
  def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    val stateTableSchema = parameters.get(STATE_TABLE_SCHEMA)
    if (stateTableSchema.isEmpty && isSecurityEnabled(sqlContext.sparkSession)) {
      val msg = s"'$STATE_TABLE_SCHEMA' is a mandatory option when security is enabled."
      throw new IllegalStateException(msg)
    }
    createSinkStateTableIfNotExist(sqlContext, stateTableSchema)
    val cc = try {
      Utils.classForName(parameters(SINK_CALLBACK)).newInstance()
    } catch {
      case _: NoSuchElementException => new DefaultSnappySinkCallback()
    }

    SnappyStoreSink(sqlContext.asInstanceOf[SnappyContext].snappySession, parameters,
      cc.asInstanceOf[SnappySinkCallback])
  }

  private def isSecurityEnabled(sparkSession: SparkSession) = {
    val connProperties = ExternalStoreUtils.validateAndGetAllProps(Some(sparkSession),
      ExternalStoreUtils.emptyCIMutableMap)
    val (user, _) = ExternalStoreUtils.getCredentials(sparkSession)
    if (!user.isEmpty) {
      true
    } else {
      try {
        val connection = DriverManager.getConnection(connProperties.url, connProperties.connProps)
        connection.close()
        false
      } catch {
        case ex: SQLException if ex.getSQLState.equals(LOGIN_FAILED) => true
      }
    }
  }

  private def createSinkStateTableIfNotExist(sqlContext: SQLContext,
      stateTableSchema: Option[String]) = {
    sqlContext.asInstanceOf[SnappyContext].snappySession.sql(s"create table if not exists" +
        s" ${stateTable(stateTableSchema)} (" +
        s" $QUERY_ID_COLUMN varchar(200)," +
        s" $BATCH_ID_COLUMN long, " +
        s" PRIMARY KEY ($QUERY_ID_COLUMN)) using row options(DISKSTORE 'GFXD-DD-DISKSTORE')")
  }

  @Override
  def shortName(): String = SnappyContext.SNAPPY_SINK_NAME
}

private[streaming] object SnappyStoreSinkProvider {

  val EVENT_TYPE_COLUMN = "_eventType"
  private val INTERNAL_SUFFIX = "snappysys_internal____"
  val SINK_STATE_TABLE = s"${INTERNAL_SUFFIX}sink_state_table"
  val TABLE_NAME = "tableName"
  val QUERY_NAME = "queryName"
  val SINK_CALLBACK = "sinkCallback"
  val STATE_TABLE_SCHEMA = "stateTableSchema"
  val CONFLATION = "conflation"
  val EVENT_COUNT_COLUMN = s"${INTERNAL_SUFFIX}event_count"
  val QUERY_ID_COLUMN = "stream_query_id"
  val BATCH_ID_COLUMN = "batch_id"
  val TEST_FAILBATCH_OPTION = s"${INTERNAL_SUFFIX}failBatch"
  val ATTEMPTS = s"${INTERNAL_SUFFIX}attempts"

  object EventType {
    val INSERT = 0
    val UPDATE = 1
    val DELETE = 2
  }

  def stateTable(schema: Option[String]): String = schema.map(s => s"$s.$SINK_STATE_TABLE")
      .getOrElse(SINK_STATE_TABLE)
}

case class SnappyStoreSink(snappySession: SnappySession,
    parameters: Map[String, String], sinkCallback: SnappySinkCallback) extends Sink with Logging {

  override def addBatch(batchId: Long, data: Dataset[Row]): Unit = {
    val message = s"queryName must be specified for ${SnappyContext.SNAPPY_SINK_NAME}."
    val queryName = snappySession.sessionCatalog
        .formatName(parameters.getOrElse(QUERY_NAME, throw new IllegalStateException(message)))
    val hashAggregateSizeIsDefault = HashAggregateSize.get(snappySession.sessionState.conf)
        .equals(HashAggregateSize.defaultValue.get)
    if (hashAggregateSizeIsDefault) {
      HashAggregateSize.set(snappySession.sessionState.conf, "10m")
    }
    try {
      processBatchWithRetries(batchId, data, queryName,
        parameters.getOrElse(ATTEMPTS, "10").toInt)
    } finally {
      if (hashAggregateSizeIsDefault) {
        HashAggregateSize.set(snappySession.sessionState.conf, HashAggregateSize.defaultValue.get)
      }
    }
  }

  private def processBatchWithRetries(batchId: Long, data: Dataset[Row], queryName: String,
      totalAttempts: Int = 10, attempt: Int = 0): Unit = {
    try {
      val possibleDuplicate = isPossibleDuplicate(queryName, batchId)
      sinkCallback.process(snappySession, parameters, batchId, convert(data), possibleDuplicate)
    } catch {
      case ex: Exception if attempt >= totalAttempts - 1 || !isRetriableException(ex) => throw ex
      case ex: Exception =>
        val sleepTime = attempt * 100
        logWarning(s"Encountered a retriable exception. Will retry processing batch after" +
            s" $sleepTime millis. Attempts left: ${totalAttempts - (attempt + 1)}", ex)
        Thread.sleep(sleepTime)
        processBatchWithRetries(batchId, data, queryName, totalAttempts, attempt + 1)
    }
  }

  private def isRetriableException(ex: Throwable): Boolean = {
    if ((ex.isInstanceOf[SQLException] &&
        SNAPPY_CATALOG_SCHEMA_VERSION_MISMATCH.equals(ex.asInstanceOf[SQLException].getSQLState))
        || ex.isInstanceOf[CatalogStaleException]) {
      true
    } else if (ex.getCause == null) {
      false
    } else {
      isRetriableException(ex.getCause)
    }
  }

  private def isPossibleDuplicate(queryName: String, batchId: Long): Boolean = {
    val stateTableSchema = parameters.get(STATE_TABLE_SCHEMA)
    val updated = snappySession.sql(s"update ${stateTable(stateTableSchema)} " +
        s"set $BATCH_ID_COLUMN=$batchId where $QUERY_ID_COLUMN='$queryName' " +
        s"and $BATCH_ID_COLUMN != $batchId")
        .collect()(0).getAs("count").asInstanceOf[Long]

    // TODO: use JDBC connection here
    var posDup = false
    if (updated == 0) {
      try {
        snappySession.insert(stateTable(stateTableSchema), Row(queryName, batchId))
        posDup = false
      }
      catch {
        case e: SQLException if e.getSQLState.equals("23505") => posDup = true
      }
    }
    posDup
  }

  /**
   * This conversion is necessary as Sink
   * documentation disallows an operation on incoming dataframe.
   * Otherwise it will break incremental planning of streaming dataframes.
   * See http://apache-spark-developers-list.1001551.n3.nabble.com/
   * Structured-Streaming-Sink-in-2-0-collect-foreach-restrictions-added-in-
   * SPARK-16020-td18118.html
   * for a detailed discussion.
   */
  private def convert(ds: DataFrame): DataFrame = {
    snappySession.internalCreateDataFrame(
      ds.queryExecution.toRdd,
      StructType(ds.schema.fields))
  }
}

import org.apache.spark.sql.snappy._

class DefaultSnappySinkCallback extends SnappySinkCallback with Logging {
  def process(snappySession: SnappySession, parameters: Map[String, String],
      batchId: Long, df: Dataset[Row], posDup: Boolean) {
    logDebug(s"Processing batchId $batchId with parameters $parameters ...")
    val tableName = snappySession.sessionCatalog.formatTableName(parameters(TABLE_NAME))
    val keyColumns = snappySession.sessionCatalog.getKeyColumnsAndPositions(tableName)
    val eventTypeColumnAvailable = df.schema.map(_.name).contains(EVENT_TYPE_COLUMN)
    val conflationEnabled = parameters.getOrElse(CONFLATION, "false").toBoolean
    if (conflationEnabled && keyColumns.isEmpty) {
      val msg = "Key column(s) or primary key must be defined on table in order " +
          "to perform conflation."
      throw new IllegalStateException(msg)
    }

    logDebug(s"keycolumns: '${keyColumns.map(p => s"${p._1.name}(${p._2})").mkString(",")}'" +
        s", eventTypeColumnAvailable:$eventTypeColumnAvailable,possible duplicate: $posDup")

    if (keyColumns.nonEmpty) {
      val dataFrame: DataFrame = persist(if (conflationEnabled) getConflatedDf else df)
      try {
        if (eventTypeColumnAvailable) {
          processDataWithEventType(dataFrame)
        } else {
          if (dataFrame.count() != 0) dataFrame.write.putInto(tableName)
        }
      } finally {
        dataFrame.unpersist()
      }
    }
    else {
      if (eventTypeColumnAvailable) {
        val msg = s"$EVENT_TYPE_COLUMN is present in data but key columns are not defined on table."
        throw new IllegalStateException(msg)
      } else {
        df.write.insertInto(tableName)
      }
    }
    // test hook for validating idempotency
    if (parameters.contains(TEST_FAILBATCH_OPTION)
        && parameters(TEST_FAILBATCH_OPTION) == "true") {
      throw new RuntimeException("dummy failure for test")
    }

    log.debug(s"Processing batchId $batchId with parameters $parameters ... Done.")

    // We are grouping by key columns and getting the last record.
    // Note that this approach will work as far as the incoming dataframe is partitioned
    // by key columns and events are available in the correct order in the respective partition.
    // If above conditions are not met in that case we will need separate ordering column(s) to
    // order the events. A new optional parameter needs to be exposed as part of the snappysink
    // API to accept the ordering column(s).
    def getConflatedDf = {
      import org.apache.spark.sql.functions._
      val keyColumnPositions = keyColumns.map(_._2)
      var index = 0
      val (keyCols, otherCols) = df.columns.toList.partition { _ =>
        val contains = keyColumnPositions.contains(index)
        index += 1
        contains
      }

      val conflatedDf: DataFrame = {
        val exprs = otherCols.map(c => last(c).alias(c)) ++
            Seq(count(lit(1)).alias(EVENT_COUNT_COLUMN))

        val columns = if (eventTypeColumnAvailable) {
          // if event type of the last event for a key is insert and there are more than one
          // events for the same key, then convert inserts to put into
          df.columns.filter(_ != EVENT_TYPE_COLUMN).map(col) ++
              Seq(when(col(EVENT_TYPE_COLUMN) === INSERT && col(EVENT_COUNT_COLUMN) > 1,
                UPDATE).otherwise(col(EVENT_TYPE_COLUMN)).alias(EVENT_TYPE_COLUMN))
        } else {
          df.columns.map(col)
        }
        df.groupBy(keyCols.head, keyCols.tail: _*)
            .agg(exprs.head, exprs.tail: _*)
            .select(columns: _*)
      }
      conflatedDf
    }

    def persist(df: DataFrame) = if (ServiceUtils.isOffHeapStorageAvailable(snappySession)) {
      df.persist(StorageLevel.OFF_HEAP)
    } else df.persist()

    def processDataWithEventType(dataFrame: DataFrame): Unit = {
      val incomingEventTypes = dataFrame.filter(dataFrame(EVENT_TYPE_COLUMN)
          .isin(INSERT, UPDATE, DELETE)).groupBy(dataFrame(EVENT_TYPE_COLUMN)).count()
          .select(EVENT_TYPE_COLUMN).collect().map(r => r(0).asInstanceOf[Int]).toSet[Int]
      if (incomingEventTypes.contains(DELETE)) {
        val deleteDf = dataFrame.filter(dataFrame(EVENT_TYPE_COLUMN) === DELETE)
            .drop(EVENT_TYPE_COLUMN)
        deleteDf.write.deleteFrom(tableName)
      }
      if (posDup) {
        if (incomingEventTypes.contains(INSERT) || incomingEventTypes.contains(UPDATE)) {
          val upsertEventTypes = List(INSERT, UPDATE)
          val upsertDf = dataFrame
              .filter(dataFrame(EVENT_TYPE_COLUMN).isin(upsertEventTypes: _*))
              .drop(EVENT_TYPE_COLUMN)
          upsertDf.write.putInto(tableName)
        }
      } else {
        if (incomingEventTypes.contains(INSERT)) {
          val insertDf = dataFrame.filter(dataFrame(EVENT_TYPE_COLUMN) === INSERT)
              .drop(EVENT_TYPE_COLUMN)
          insertDf.write.insertInto(tableName)
        }
        if (incomingEventTypes.contains(UPDATE)) {
          val updateDf = dataFrame.filter(dataFrame(EVENT_TYPE_COLUMN) === UPDATE)
              .drop(EVENT_TYPE_COLUMN)
          updateDf.write.putInto(tableName)
        }
      }
    }
  }
}