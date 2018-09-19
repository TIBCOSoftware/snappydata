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

package org.apache.spark.sql.streaming

import java.sql.SQLException
import java.util.{NoSuchElementException, Properties}

import org.apache.log4j.Logger

import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.Constants.{deleteEventType, eventTypeColumn, insertEventType, tableNameProperty, updateEventType}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SnappySession, _}
import org.apache.spark.util.Utils

trait SnappySinkCallback {

  def process(snappySession: SnappySession, sinkProps: Properties,
              batchId: Long, df: Dataset[Row], possibleDuplicate: Boolean = false): Unit
}

class SnappyStoreSinkProvider extends StreamSinkProvider with DataSourceRegister {

  @Override
  def createSink(
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode): Sink = {
    val props = new Properties()
    parameters.foreach { case (k, v) => props.setProperty(k, v) }
    createSinkStateTableIfNotExist(sqlContext)
    val cc = try {
      Utils.classForName(parameters("sink")).newInstance()
    } catch {
      case _: NoSuchElementException => new DefaultSnappySinkCallback()
    }

    SnappyStoreSink(sqlContext.asInstanceOf[SnappyContext].snappySession, props,
      cc.asInstanceOf[SnappySinkCallback])
  }

  private def createSinkStateTableIfNotExist(sqlContext: SQLContext) = {
    sqlContext.asInstanceOf[SnappyContext].snappySession.sql(s"create table if not exists" +
      s" ${Constants.sinkStateTable}(" +
      " sink_id varchar(200)," +
      " batchid long, " +
      " PRIMARY KEY (sink_id)) using row options(DISKSTORE 'GFXD-DD-DISKSTORE')")
  }

  @Override
  def shortName(): String = "snappysink"

}

case class SnappyStoreSink(snappySession: SnappySession,
                           properties: Properties, sinkCallback: SnappySinkCallback) extends Sink {

  override def addBatch(batchId: Long, data: Dataset[Row]): Unit = {
    val sinkId = properties.getProperty("sinkid").toUpperCase

    val updated = snappySession.sql(s"update ${Constants.sinkStateTable} " +
      s"set batchid=$batchId where sink_id='$sinkId' and batchid != $batchId")
      .collect()(0).getAs("count").asInstanceOf[Long]

    // TODO: use JDBC connection here
    var posDup = false

    if (updated == 0) {
      try {
        snappySession.insert(Constants.sinkStateTable, Row(sinkId, batchId))
        posDup = false
      }
      catch {
        case _: SQLException => posDup = true
        case exception: Throwable => throw exception
      }
    }

    sinkCallback.process(snappySession, properties, batchId, convert(data), posDup)
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
  def convert(ds: DataFrame): DataFrame = {
    snappySession.internalCreateDataFrame(
      ds.queryExecution.toRdd,
      StructType(ds.schema.fields))
  }
}

object DefaultSnappySinkCallback {
  private val log = Logger.getLogger(classOf[DefaultSnappySinkCallback].getName)
}

import org.apache.spark.sql.snappy._

class DefaultSnappySinkCallback extends SnappySinkCallback {
  def process(snappySession: SnappySession, sinkProps: Properties,
              batchId: Long, df: Dataset[Row], posDup: Boolean) {
    val snappyTable = sinkProps.getProperty(tableNameProperty).toUpperCase

    DefaultSnappySinkCallback.log.debug(s"Processing for $snappyTable and batchId $batchId")

    val tableName = sinkProps.getProperty(tableNameProperty).toUpperCase
    val keyColumnsDefined = !snappySession.sessionCatalog.getKeyColumns(tableName).head(1).isEmpty
    val eventTypeColumnAvailable = df.schema.map(_.name).contains(eventTypeColumn)
    if (keyColumnsDefined) {
      if (eventTypeColumnAvailable) {
        // TODO: handle scenario when a batch contain multiple records with
        // same key columns using conflation.
        val deleteDf = df.filter(df(eventTypeColumn) === deleteEventType)
          .drop(eventTypeColumn)
        deleteDf.write.deleteFrom(tableName)
        if (posDup) {
          val upsertEventTypes = List(insertEventType, updateEventType)
          val upsertDf = df.filter(df(eventTypeColumn).isin(upsertEventTypes: _*))
              .drop(eventTypeColumn)
          upsertDf.write.putInto(tableName)
        } else {
          val insertDf = df.filter(df(eventTypeColumn) === insertEventType)
            .drop(eventTypeColumn)
          insertDf.write.insertInto(tableName)
          val updateDf = df.filter(df(eventTypeColumn) === updateEventType)
            .drop(eventTypeColumn)
          updateDf.write.putInto(tableName)
        }
      } else {
        df.write.putInto(tableName)
      }
    }
    else {
      if (eventTypeColumnAvailable) {
        val msg = s"$eventTypeColumn is present in data but key columns are not defined on table."
        throw new RuntimeException(msg)
      } else {
        df.write.insertInto(tableName)
      }
    }
  }

  private def dropEventTypeColumn(df: Dataset[Row]) = {
    df.drop(eventTypeColumn).select("*")
  }

}

object Constants {
  val eventTypeColumn = "_eventType"
  val insertEventType = 0
  val updateEventType = 1
  val deleteEventType = 2
  val tableNameProperty = "tablename"
  val sinkStateTable = s"SNAPPY_SINK_STATE_TABLE"
}