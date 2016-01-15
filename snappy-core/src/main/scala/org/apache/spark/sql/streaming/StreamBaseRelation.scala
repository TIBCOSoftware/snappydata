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

import org.apache.spark.Logging
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.sources.{BaseRelation, DestroyRelation, TableScan}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils

abstract class StreamBaseRelation(options: Map[String, String]) extends BaseRelation with StreamPlan
with TableScan with DestroyRelation with Serializable with Logging {

  @transient val context = SnappyStreamingContext.getActive().get

  @transient var rowStream: DStream[InternalRow] = _

  @transient val tableName = options("tableName")

  val storageLevel = options.get("storageLevel")
      .map(StorageLevel.fromString)
      .getOrElse(StorageLevel.MEMORY_AND_DISK_SER_2)

  val rowConverter = {
    try {
      val clz = Utils.getContextOrSparkClassLoader.loadClass(options("rowConverter"))
      clz.newInstance().asInstanceOf[StreamToRowsConverter]
    } catch {
      case e: Exception => sys.error(s"Failed to load class : ${e.toString}")
    }
  }

  override def buildScan(): RDD[Row] = {
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    if (rowStream.generatedRDDs.isEmpty) {
      new EmptyRDD[Row](sqlContext.sparkContext)
    } else {
      rowStream.generatedRDDs.maxBy(_._1)._2.map(converter(_)).asInstanceOf[RDD[Row]]
    }

    /* val currentRDD = rowStream.generatedRDDs.maxBy(_._1)._2
      val rdds = Seq(rowStream.generatedRDDs.values.toArray: _*)
      val urdds = new UnionRDD(sqlContext.sparkContext, rdds)
      urdds.map(converter(_)).asInstanceOf[RDD[Row]]
    } */
  }

  override def destroy(ifExists: Boolean): Unit = {
    val catalog = context.snappyContext.catalog
    val qualifiedTable = catalog.newQualifiedTableName(tableName)
    catalog.tables -= qualifiedTable
    StreamBaseRelation.tableToStream -= tableName
  }

  def truncate(): Unit = {
    throw new IllegalAccessException("Stream tables cannot be truncated")
  }
}

private object StreamBaseRelation extends Logging {

  private var tableToStream =
    new scala.collection.mutable.HashMap[String, DStream[InternalRow]]()

  val LOCK = new Object()

  var validTime: Time = null

  def setValidTime(time: Time): Unit = {
    if (validTime == null) {
      validTime = time
    } else if (validTime != time) {
      validTime = time
    } else {
    }
  }

  def setRowStream(tableName: String, stream: DStream[InternalRow]): Unit = {
    tableToStream += (tableName -> stream)
    // TODO Yogesh, this is required from snappy-shell, need to get rid of this
    stream.foreachRDD { rdd => rdd }
  }

  def getRowStream(tableName: String): DStream[InternalRow] = {
    tableToStream.getOrElse(tableName, null)
  }
}