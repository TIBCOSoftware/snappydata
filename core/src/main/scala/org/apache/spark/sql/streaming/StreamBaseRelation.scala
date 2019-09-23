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

import scala.collection.mutable

import io.snappydata.sql.catalog.SnappyExternalCatalog

import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.{Row, SparkSupport}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{SnappyStreamingContext, StreamUtils, StreamingContextState, Time}
import org.apache.spark.{Logging, util}

abstract class StreamBaseRelation(opts: Map[String, String]) extends DestroyRelation
    with StreamPlan with TableScan with Serializable with Logging with SparkSupport {

  final def context: SnappyStreamingContext =
    SnappyStreamingContext.getInstance().getOrElse(
      throw new IllegalStateException("No initialized streaming context"))

  protected val options: Map[String, String] = internals.newCaseInsensitiveMap(opts)

  @transient val tableName = options(SnappyExternalCatalog.DBTABLE_PROPERTY)

  val storageLevel: StorageLevel = options.get("storageLevel")
      .map(StorageLevel.fromString)
      .getOrElse(StorageLevel.MEMORY_AND_DISK_SER_2)

  val rowConverter: StreamToRowsConverter = {
    try {
      val clz = util.Utils.getContextOrSparkClassLoader.loadClass(
        options("rowConverter"))
      clz.newInstance().asInstanceOf[StreamToRowsConverter]
    } catch {
      case e: Exception => sys.error(s"Failed to load class : ${e.toString}")
    }
  }

  protected def createRowStream(): DStream[InternalRow]

  @transient override final lazy val rowStream: DStream[InternalRow] =
    StreamBaseRelation.getOrCreateRowStream(tableName, createRowStream)

  override val needConversion: Boolean = false

  override def buildScan(): RDD[Row] = {
    if (context.getState() == StreamingContextState.STOPPED) {
      throw new IllegalStateException("StreamingContext has stopped")
    }
    val rdds = StreamUtils.getGeneratedRDDs(rowStream)
    if (rdds.isEmpty) {
      new EmptyRDD[Row](sqlContext.sparkContext)
    } else {
      rdds.maxBy(_._1)._2.asInstanceOf[RDD[Row]]
    }

    /* val currentRDD = rowStream.generatedRDDs.maxBy(_._1)._2
      val rdds = Seq(rowStream.generatedRDDs.values.toArray: _*)
      val urdds = new UnionRDD(sqlContext.sparkContext, rdds)
      urdds.map(converter(_)).asInstanceOf[RDD[Row]]
    } */
  }

  override def destroy(ifExists: Boolean): Unit = {
    StreamBaseRelation.removeStream(tableName).foreach(
      StreamBaseRelation.stopStream)
  }

  def truncate(): Unit = {
    throw Utils.analysisException("Stream tables cannot be truncated")
  }
}

private object StreamBaseRelation extends Logging {

  private[this] val tableToStream =
    new mutable.HashMap[String, DStream[InternalRow]]()

  private[this] val LOCK = new Object()

  var validTime: Time = _

  def setValidTime(time: Time): Unit = {
    if (validTime == null) {
      validTime = time
    } else if (validTime != time) {
      validTime = time
    } else {
    }
  }

  private def getOrCreateRowStream(tableName: String,
      createStream: () => DStream[InternalRow]): DStream[InternalRow] =
    LOCK.synchronized {
      tableToStream.get(tableName) match {
        case None =>
          val stream = createStream()
          tableToStream += (tableName -> stream)
          stream
        case Some(stream) => stream
      }
    }

  private def removeStream(tableName: String): Option[DStream[InternalRow]] =
    LOCK.synchronized {
      tableToStream.remove(tableName)
    }

  def stopStream(stream: DStream[_]): Unit = stream match {
    case inputStream: ReceiverInputDStream[_] =>
      try {
        val receiver = inputStream.getReceiver()
        if (receiver != null && !receiver.isStopped()) {
          receiver.stop("destroyRelation")
        }
      } finally {
        inputStream.stop()
      }
    case inputStream: InputDStream[_] => inputStream.stop()
    case _ => // nothing
  }

  private[streaming] def clearStreams(): Unit = LOCK.synchronized {
    tableToStream.clear()
  }
}
