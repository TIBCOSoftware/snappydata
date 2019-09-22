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
package org.apache.spark.streaming

import java.util.concurrent.atomic.AtomicReference

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import com.pivotal.gemfirexd.Attribute
import io.snappydata.Constant
import io.snappydata.sql.catalog.CatalogObjectType
import org.apache.hadoop.conf.Configuration

import org.apache.spark.annotation.Experimental
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.streaming.{SchemaDStream, StreamBaseRelation, StreamSqlHelper}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SnappyContext, SnappySession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Main entry point for SnappyData extensions to Spark Streaming.
 * A SnappyStreamingContext extends Spark's [[org.apache.spark.streaming.StreamingContext]]
 * to provides an ability to manipulate SQL like query on
 * [[org.apache.spark.streaming.dstream.DStream]]. You can apply schema and
 * register continuous SQL queries(CQ) over the data streams.
 * A single shared SnappyStreamingContext makes it possible to re-use Executors
 * across client connections or applications.
 */
class SnappyStreamingContext protected[spark](
    sc_ : SparkContext,
    cp_ : Checkpoint,
    batchDur_ : Duration, private val reuseSnappySession: Option[SnappySession] = None,
    private val currentSnappySession: Option[SnappySession] = None)
    extends StreamingContext(sc_, cp_, batchDur_) with Serializable {

  self =>


  if (sc_ == null && cp_ == null) {
    throw new Exception("Snappy Streaming cannot be initialized with " +
        "both SparkContext and checkpoint as null")
  }

  val snappySession: SnappySession = reuseSnappySession.getOrElse(new SnappySession(sc))
  currentSnappySession.foreach(csn => {
    val attrs = Attribute.USERNAME_ATTR -> Attribute.PASSWORD_ATTR
    val smartAttrs = (Constant.SPARK_STORE_PREFIX + attrs._1) ->
        (Constant.SPARK_STORE_PREFIX + attrs._2)
    // exists (or find) instead of foreach to break out on first match
    Seq(attrs, smartAttrs).exists { case (userKey, passKey) =>
      if (!csn.conf.get(userKey, "").isEmpty) {
        snappySession.sessionState.conf.setConfString(userKey, csn.conf.get(userKey))
        snappySession.sessionState.conf.setConfString(passKey, csn.conf.get(passKey, ""))
        true
      } else false
    }
  })

  val snappyContext: SnappyContext = snappySession.snappyContext

  SnappyStreamingContext.setInstanceContext(self)

  /**
   * Create a SnappyStreamingContext using an existing SparkContext.
   *
   * @param sparkContext  existing SparkContext
   * @param batchDuration the time interval at which streaming data will be divided into batches
   */
  def this(sparkContext: SparkContext, batchDuration: Duration) = {
    this(sparkContext, null, batchDuration)
  }

  def this(snappySession: SnappySession, batchDuration: Duration) = {
    this(snappySession.snappyContext.sparkContext, null, batchDuration, Some(snappySession))
  }

  /**
   * Create a SnappyStreamingContext by providing the configuration necessary
   * for a new SparkContext.
   *
   * @param conf          a org.apache.spark.SparkConf object specifying Spark parameters
   * @param batchDuration the time interval at which streaming data will be divided into batches
   */
  def this(conf: SparkConf, batchDuration: Duration) = {
    this(StreamingContext.createNewSparkContext(conf), null, batchDuration)
  }


  /**
   * Recreate a SnappyStreamingContext from a checkpoint file.
   *
   * @param path       Path to the directory that was specified as the checkpoint directory
   * @param hadoopConf Optional, configuration object if necessary for reading from
   *                   HDFS compatible filesystems
   */
  def this(path: String, hadoopConf: Configuration) =
    this(null, CheckpointReader.read(path, new SparkConf(), hadoopConf).get, null)

  /**
   * Recreate a SnappyStreamingContext from a checkpoint file.
   *
   * @param path Path to the directory that was specified as the checkpoint directory
   */
  def this(path: String) = this(path, SparkHadoopUtil.get.conf)

  /**
   * Recreate a SnappyStreamingContext from a checkpoint file using an existing SparkContext.
   *
   * @param path         Path to the directory that was specified as the checkpoint directory
   * @param sparkContext Existing SparkContext
   */
  def this(path: String, sparkContext: SparkContext) = {
    this(
      sparkContext,
      CheckpointReader.read(path, sparkContext.conf, sparkContext.hadoopConfiguration).get,
      null)
  }


  /**
   * Start the execution of the streams.
   * Also registers population of AQP tables from stream tables if present.
   *
   * @throws IllegalStateException if the StreamingContext is already stopped
   */
  override def start(): Unit = synchronized {
    if (getState() == StreamingContextState.INITIALIZED) {
      registerStreamTables()
      // register population of AQP tables from stream tables
      snappySession.contextFunctions.aqpTablePopulator()
    }
    SnappyStreamingContext.setActiveContext(self)
    super.start()
  }

  def registerStreamTables(): Unit = {
    // register dummy output transformations for the stream tables
    // so that the streaming context starts
    snappySession.sessionState.catalog.getDataSourceRelations[StreamBaseRelation](
      CatalogObjectType.Stream).foreach(_.rowStream.foreachRDD(_ => Unit))
  }

  override def stop(stopSparkContext: Boolean,
      stopGracefully: Boolean): Unit = {
    try {
      super.stop(stopSparkContext, stopGracefully)
      SnappyStreamingContext.setActiveContext(null)
      SnappyStreamingContext.setInstanceContext(null)
    } finally {
      // snappySession.clearCache()
      if (stopSparkContext) {
        snappySession.clear()
      }

      StreamSqlHelper.clearStreams()
    }
  }

  def sql(sqlText: String): DataFrame = {
    snappySession.sqlUncached(sqlText)
  }

  /**
   * Registers and executes given SQL query and
   * returns [[SchemaDStream]] to consume the results
   *
   * @param queryStr the query to register
   */
  def registerCQ(queryStr: String): SchemaDStream = {
    val plan = sql(queryStr).queryExecution
    // force optimization right away
    assert(plan.optimizedPlan != null)
    val dStream = new SchemaDStream(self, plan)
    // register a dummy task so that the DStream gets started
    // TODO: need to remove once we add proper registration of registerCQ
    // streams in catalog and possible AQP structures on top
    dStream.foreachRDD((_, _) => Unit)
    dStream
  }

  def getSchemaDStream(tableName: String): SchemaDStream = {
    StreamSqlHelper.getSchemaDStream(self, tableName)
  }

  /**
   * Creates a [[SchemaDStream]] from an DStream of Product (e.g. case classes).
   */
  def createSchemaDStream[A <: Product : TypeTag]
  (stream: DStream[A]): SchemaDStream = {
    StreamSqlHelper.createSchemaDStream(self, stream)
  }

  def createSchemaDStream(rowStream: DStream[Row], schema: StructType): SchemaDStream = {
    StreamSqlHelper.createSchemaDStream(self, rowStream, schema)
  }


}

object SnappyStreamingContext extends Logging {

  /**
   * Lock that guards activation of a StreamingContext as well as access to the singleton active
   * SnappyStreamingContext in getActiveOrCreate().
   */
  private val ACTIVATION_LOCK = new Object()

  private val activeContext = new AtomicReference[SnappyStreamingContext](null)

  /**
   * This one holds any instance created by a specified configuration.This is
   * different from activeContext . ActiveContext holds only that context which
   * has been started
   */
  private val instanceContext = new AtomicReference[SnappyStreamingContext](null)

  private def setActiveContext(snsc: SnappyStreamingContext): Unit = {
    ACTIVATION_LOCK.synchronized {
      activeContext.set(snsc)
    }
  }

  private def setInstanceContext(snsc: SnappyStreamingContext): Unit = {
    ACTIVATION_LOCK.synchronized {
      instanceContext.set(snsc)
    }
  }

  /**
   * :: Experimental ::
   *
   * Get the currently created context, it may be started or not, but never stopped.
   */
  @Experimental
  def getInstance(): Option[SnappyStreamingContext] = {
    ACTIVATION_LOCK.synchronized {
      Option(instanceContext.get())
    }
  }

  /**
   * :: Experimental ::
   *
   * Get the currently active context, if there is one. Active means started but not stopped.
   */
  @Experimental
  def getActive: Option[SnappyStreamingContext] = {
    ACTIVATION_LOCK.synchronized {
      Option(activeContext.get())
    }
  }

  /**
   * :: Experimental ::
   * Either return the "active" StreamingContext (that is, started but not stopped), or create a
   * new StreamingContext that is started by the creating function
   *
   * @param creatingFunc Function to create a new StreamingContext
   */
  @Experimental
  def getActiveOrCreate(creatingFunc: () => SnappyStreamingContext): SnappyStreamingContext = {
    ACTIVATION_LOCK.synchronized {
      getActive.getOrElse {
        creatingFunc()
      }
    }
  }

  /**
   * :: Experimental ::
   *
   * Either get the currently active StreamingContext (that is, started but not stopped),
   * OR recreate a StreamingContext from checkpoint data in the given path. If checkpoint data
   * does not exist in the provided, then create a new StreamingContext by calling the provided
   * `creatingFunc`.
   *
   * @param checkpointPath Checkpoint directory used in an earlier StreamingContext program
   * @param creatingFunc   Function to create a new StreamingContext
   * @param hadoopConf     Optional Hadoop configuration if necessary for reading from the
   *                       file system
   * @param createOnError  Optional, whether to create a new StreamingContext if there is an
   *                       error in reading checkpoint data. By default, an exception will be
   *                       thrown on error.
   */
  @Experimental
  def getActiveOrCreate(
      checkpointPath: String,
      creatingFunc: () => SnappyStreamingContext,
      hadoopConf: Configuration = SparkHadoopUtil.get.conf,
      createOnError: Boolean = false
  ): SnappyStreamingContext = {
    ACTIVATION_LOCK.synchronized {
      getActive.getOrElse {
        getOrCreate(checkpointPath, creatingFunc,
          hadoopConf, createOnError)
      }
    }
  }

  /**
   * Either recreate a SnappyStreamingContext from checkpoint data or create a
   * new SnappyStreamingContext. If checkpoint data exists in the provided
   * `checkpointPath`, then SnappyStreamingContext will be recreated from the
   * checkpoint data. If the data does not exist, then the StreamingContext
   * will be created by called the provided `creatingFunc`.
   *
   * @param checkpointPath Checkpoint directory used in an earlier StreamingContext program
   * @param creatingFunc   Function to create a new SnappyStreamingContext
   * @param hadoopConf     Optional Hadoop configuration if necessary for reading from the
   *                       file system
   * @param createOnError  Optional, whether to create a new SnappyStreamingContext if there is an
   *                       error in reading checkpoint data. By default, an exception will be
   *                       thrown on error.
   */

  def getOrCreate(
      checkpointPath: String,
      creatingFunc: () => SnappyStreamingContext,
      hadoopConf: Configuration = SparkHadoopUtil.get.conf,
      createOnError: Boolean = false
  ): SnappyStreamingContext = {
    val checkpointOption = CheckpointReader.read(
      checkpointPath, new SparkConf(), hadoopConf, createOnError)
    checkpointOption.map(new SnappyStreamingContext(null, _, null)).
        getOrElse(creatingFunc())
  }

  /**
   * Either recreate a SnappyStreamingContext from checkpoint data or create a
   * new SnappyStreamingContext. If checkpoint data exists in the provided
   * `checkpointPath`, then SnappyStreamingContext will be recreated from the
   * checkpoint data. If the data does not exist, then the StreamingContext
   * will be created by called the provided `creatingFunc`.
   *
   * @param checkpointPath Checkpoint directory used in an earlier StreamingContext program
   * @param creatingFunc   Function to create a new SnappyStreamingContext
   * @param currentSession Current SnappySession instance from which to use the credentials
   * @param hadoopConf     Optional Hadoop configuration if necessary for reading from the
   *                       file system
   * @param createOnError  Optional, whether to create a new SnappyStreamingContext if there is an
   *                       error in reading checkpoint data. By default, an exception will be
   *                       thrown on error.
   */
  def getOrCreateWithUseCredential(
      checkpointPath: String,
      creatingFunc: () => SnappyStreamingContext,
      currentSession: SnappySession,
      hadoopConf: Configuration = SparkHadoopUtil.get.conf,
      createOnError: Boolean = false
  ): SnappyStreamingContext = {
    val checkpointOption = CheckpointReader.read(
      checkpointPath, new SparkConf(), hadoopConf, createOnError)
    checkpointOption.map(new SnappyStreamingContext(null, _, null, None, Option(currentSession))).
        getOrElse(creatingFunc())
  }
}


private class SnappyStreamingContextPythonHelper {
  /**
   * This is a private method only for Python to implement `getOrCreate`.
   */
  def tryRecoverFromCheckpoint(checkpointPath: String): Option[SnappyStreamingContext] = {
    val checkpointOption = CheckpointReader.read(
      checkpointPath, new SparkConf(), SparkHadoopUtil.get.conf)
    checkpointOption.map(new SnappyStreamingContext(null, _, null))
  }
}
