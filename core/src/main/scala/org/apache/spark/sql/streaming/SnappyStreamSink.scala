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
import java.util.{Properties, UUID}

import org.apache.log4j.Logger
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SnappySession, _}
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

    val cc = parameters("sink") match {
      case null => new DefaultSnappySink()
      case _ => Utils.classForName(parameters("sink")).newInstance()
    }

    // TODO: For default user can't give sinkId, we can generate and store it
    // and for each sink can assign one.
    // i.e everytime we create one Sink, we can get from persisted table
    val sinkId = UUID.randomUUID

    SnappyStoreSink(sqlContext.asInstanceOf[SnappyContext].snappySession, props,
      cc.asInstanceOf[SnappyStreamSink])
  }

  private def createSinkStateTableIfNotExist(sqlContext: SQLContext) = {
    sqlContext.asInstanceOf[SnappyContext].snappySession.sql(s"create table if not exists" +
      s" ${DefaultSnappySink.sinkStateTable}(" +
      " sink_id varchar(200)," +
      " batchid long, " +
      " status varchar(10), " +
      " PRIMARY KEY (Sink_Name)) using row options(DISKSTORE 'GFXD-DD-DISKSTORE')")
  }

  @Override
  def shortName(): String = "snappystore"

}

case class SnappyStoreSink(snappySession: SnappySession,
                           properties: Properties, sink: SnappyStreamSink) extends Sink {

  @Override
  def addBatch(batchId: Long, data: Dataset[Row]): Unit = {
    // TODO: before callback: store the batchID in the table, with start
    // change the callback to have an extra column indicating possibleDuplicate
    val sinkId = properties.getProperty("SINK_ID").toUpperCase

    val result = snappySession.sql(s"select status from " +
      s"${DefaultSnappySink.sinkStateTable} where sink_id=$sinkId and batchid = $batchId")

    val posDup = result.collect().length match {
      case 0 => false
      case 1 => true
    }
    try {
      snappySession.insert(DefaultSnappySink.sinkStateTable, Row(sinkId, batchId, "start"))
    }
    catch {
      case e : SQLException => // already exists
      case exception: Throwable => throw exception
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
  private val log = Logger.getLogger(classOf[DefaultSnappySink].getName)
}

import org.apache.spark.sql.snappy._

class DefaultSnappySink extends SnappyStreamSink {
  def process(snappySession: SnappySession, sinkProps: Properties,
              batchId: Long, df: Dataset[Row], posDup : Boolean) {
    val snappyTable = sinkProps.getProperty("tablename").toUpperCase

    DefaultSnappySink.log.debug("Processing for " + snappyTable + " batchId " + batchId)
    /* --------------[ Preferred Way ] ---------------- */ df.cache
    val snappyCustomerDelete = df.filter("\"_eventType\" = 3").drop("_eventType")
    if (snappyCustomerDelete.count > 0) {
      snappyCustomerDelete.write.deleteFrom("APP." + snappyTable)
    }

    val snappyCustomerUpsert = df.filter("\"_eventType\" = 1 OR \"_eventType\" = 2")
      .drop("_eventType")

    snappyCustomerUpsert.write.putInto("APP." + snappyTable)
  }
}


