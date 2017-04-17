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

package org.apache.spark.sql.execution.streaming

import javax.annotation.concurrent.GuardedBy

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.snappy._

import scala.collection.mutable.ArrayBuffer

/** A sink that stores the results in SnappyData store.
  * This [[org.apache.spark.sql.execution.streaming.Sink]] is in memory only as of now.
  */

class SnappySink(/* val schema: StructType, */ session: SnappySession,
                 outputMode: OutputMode, tableName: String)
  extends Sink with Logging {

  /** An order list of batches that have been written to this [[Sink]]. */
  @GuardedBy("this")
  private val batches = new ArrayBuffer[Long]()

  def latestBatchId: Option[Long] = synchronized {
    batches.lastOption
  }

  def clear(): Unit = synchronized {
    batches.clear()
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    println("addBatch( " + batchId + " )")
    val notCommitted = synchronized {
      latestBatchId.isEmpty || batchId > latestBatchId.get
    }
    if (notCommitted) {
      logDebug(s"Committing batch $batchId to $this")
      outputMode match {
        case InternalOutputModes.Append | InternalOutputModes.Update =>
          data.write.insertInto(tableName)
          synchronized {
            batches += batchId
          }

        case InternalOutputModes.Complete =>
          val df = session.createDataFrame(
            session.sparkContext.parallelize(data.collect()), data.schema)
          df.write.putInto(tableName)
          synchronized {
            batches.clear()
            batches += batchId
          }

        case _ =>
          throw new IllegalArgumentException(
            s"Output mode $outputMode is not supported by SnappySink")
      }
    } else {
      logDebug(s"Skipping already committed batch: $batchId")
    }
  }

  override def toString(): String = "SnappySink"

  // currently no way to pass the schema from DataSource.
  // may need to suggest spark team to modify the StreamSinkProvider
  var schema: StructType = StructType(Array(StructField("id", IntegerType, false),
    StructField("cnt", IntegerType, false)))

}

case class SnappySinkPlan(sink: SnappySink, output: Seq[Attribute]) extends LeafNode {
  def this(sink: SnappySink) = this(sink, sink.schema.toAttributes)

  private val sizePerRow = sink.schema.toAttributes.map(_.dataType.defaultSize).sum

  override def statistics: Statistics = Statistics(sizePerRow * 1 /* table size */)
}

class SnappyStreamSinkProvider extends StreamSinkProvider with DataSourceRegister {
  def createSink(
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode): Sink = {
    val spark = SparkSession.getActiveSession.orNull
    val session = SnappySession.getOrCreate(spark.sparkContext)
    val queryName = parameters.get("queryName").get
    val sink = new SnappySink(session, outputMode, queryName)
    // val resultDf = Dataset.ofRows(spark, new SnappySinkPlan(sink))
    outputMode match {
      case InternalOutputModes.Append | InternalOutputModes.Update =>
        session.snappyContext.createTable(queryName, "column",
          sink.schema, Map.empty[String, String], false)
      case InternalOutputModes.Complete =>
        val schemaDDL = "(id INT NOT NULL PRIMARY KEY, cnt INT)"
        session.snappyContext.createTable(queryName, "row",
          schemaDDL, Map.empty[String, String], false)
    }
    // resultDf.write.insertInto("snappyTable") // Pass the queryName somehow
    sink
  }

  def shortName(): String = "snappy"
}
