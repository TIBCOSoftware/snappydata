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

trait SnappyStreamSink {

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
    // TODO: Check here for the sink provided, if not then add DefaultSnappySink
    // Get all other property too, e.g sink name
    // create a persistent metadata table if not exists and then check if there
    // is a row against that sink name
    // currently no check for duplicate.
    createSinkStateTableIfNotExist(sqlContext)

    val cc = try {
      Utils.classForName(parameters("sink")).newInstance()
    } catch {
      case _: NoSuchElementException => new DefaultSnappySink()
    }

    SnappyStoreSink(sqlContext.asInstanceOf[SnappyContext].snappySession, props,
      cc.asInstanceOf[SnappyStreamSink])
  }

  private def createSinkStateTableIfNotExist(sqlContext: SQLContext) = {
    sqlContext.asInstanceOf[SnappyContext].snappySession.sql(s"create table if not exists" +
        s" ${DefaultSnappySink.sinkStateTable}(" +
        " sink_id varchar(200)," +
        " batchid long, " +
        " status varchar(10), " +
        " PRIMARY KEY (sink_id)) using row options(DISKSTORE 'GFXD-DD-DISKSTORE')")
  }

  @Override
  def shortName(): String = "snappystore"

}

case class SnappyStoreSink(snappySession: SnappySession,
    properties: Properties, sink: SnappyStreamSink) extends Sink {

  override def addBatch(batchId: Long, data: Dataset[Row]): Unit = {
    // TODO: before callback: store the batchID in the table, with start
    // change the callback to have an extra column indicating possibleDuplicate
    val sinkId = properties.getProperty("sinkid").toUpperCase

    val updated = snappySession.sql(s"update ${DefaultSnappySink.sinkStateTable} " +
        s"set batchid=$batchId where sink_id='$sinkId' and batchid != $batchId")
        .collect()(0).getAs("count").asInstanceOf[Long]

    // TODO: use JDBC connection here
    var posDup = false

    if (updated == 0) {
      try {
        snappySession.insert(DefaultSnappySink.sinkStateTable, Row(sinkId, batchId, "start"))
        posDup = false
      }
      catch {
        case _: SQLException => posDup = true
        case exception: Throwable => throw exception
      }
    }

    sink.process(snappySession, properties, batchId, convert(data), posDup)
    // TODO: after callback: store the batchID in the table, with end(small optimization)
    // snappySession.sql(s"update ${DefaultSnappySink.sinkStateTable} set status = \"end\" where " +
    //  s"sinkid=$sinkId and batchid = $batchId ")
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

object DefaultSnappySink {
  val sinkStateTable = s"SNAPPY_SINK_STATE_TABLE"
  val log = Logger.getLogger(classOf[DefaultSnappySink].getName)
}

import org.apache.spark.sql.snappy._

class DefaultSnappySink extends SnappyStreamSink {
  def process(snappySession: SnappySession, sinkProps: Properties,
      batchId: Long, df: Dataset[Row], posDup: Boolean) {
    val snappyTable = sinkProps.getProperty("tablename").toUpperCase

    DefaultSnappySink.log.debug("Processing for " + snappyTable + " batchId " + batchId)

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
          val insertDf = df.filter(df(eventTypeColumn).isin(Seq(insertEventType, updateEventType)))
              .drop(eventTypeColumn)
          insertDf.write.insertInto(tableName)
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
}


