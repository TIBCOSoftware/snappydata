/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

package org.apache.spark.streaming.api.java

import com.google.common.base.Optional
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.{Function0 => JFunction0}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.streaming.{SchemaDStream, StreamSqlHelper}
import org.apache.spark.sql.{Dataset, Row, SnappySession}
import org.apache.spark.streaming.{Checkpoint, CheckpointReader, Duration, SnappyStreamingContext, StreamingContext}

class JavaSnappyStreamingContext(val snsc: SnappyStreamingContext)
    extends JavaStreamingContext(snsc) {

  def snappySession: SnappySession = snsc.snappySession

  /**
   * Create a JavaSnappyStreamingContext using an existing SparkContext.
   * @param sparkContext existing SparkContext
   * @param checkpoint checkpoint directory
   * @param batchDuration the time interval at which streaming data will be divided
   *                      into batches
   */
  def this(sparkContext: JavaSparkContext, checkpoint: Checkpoint, batchDuration: Duration) = {
    this(new SnappyStreamingContext(sparkContext.sc, checkpoint, batchDuration))
  }

  /**
   * Create a JavaSnappyStreamingContext using an existing SparkContext.
   * @param sparkContext existing SparkContext
   * @param batchDuration the time interval at which streaming data will be divided
   *                      into batches
   */
  def this(sparkContext: JavaSparkContext, batchDuration: Duration) = {
    this(new SnappyStreamingContext(sparkContext.sc, null, batchDuration))
  }

  /**
   * Create a JavaSnappyStreamingContext by providing the configuration necessary for a
   * new SparkContext.
   * @param conf a org.apache.spark.SparkConf object specifying Spark parameters
   * @param batchDuration the time interval at which streaming data will be divided into batches
   */
  def this(conf: SparkConf, batchDuration: Duration) = {
    this(new SnappyStreamingContext(StreamingContext.createNewSparkContext(conf), null
      , batchDuration))
  }


  /**
   * Recreate a JavaSnappyStreamingContext from a checkpoint file.
   * @param path Path to the directory that was specified as the checkpoint directory
   * @param hadoopConf Optional, configuration object if necessary for reading from
   *                   HDFS compatible filesystems
   */
  def this(path: String, hadoopConf: Configuration) =
    this(new SnappyStreamingContext(null, CheckpointReader.read(path, new SparkConf(),
      hadoopConf).get, null))

  /**
   * Recreate a JavaSnappyStreamingContext from a checkpoint file.
   * @param path Path to the directory that was specified as the checkpoint directory
   */
  def this(path: String) = this(new SnappyStreamingContext(path, SparkHadoopUtil.get.conf))

  /**
   * Recreate a JavaSnappyStreamingContext from a checkpoint file using an existing SparkContext.
   * @param path Path to the directory that was specified as the checkpoint directory
   * @param sparkContext Existing SparkContext
   */
  def this(path: String, sparkContext: JavaSparkContext) = {
    this(new SnappyStreamingContext(
      sparkContext,
      CheckpointReader.read(path, sparkContext.conf, sparkContext.hadoopConfiguration).get,
      null))
  }

  /**
   * Start the execution of the streams.
   * Also registers population of AQP tables from stream tables if present.
   *
   * @throws IllegalStateException if the JavaSnappyStreamingContext is already stopped
   */
  override def start(): Unit = snsc.start()

  override def stop(stopSparkContext: Boolean, stopGracefully: Boolean): Unit = {
    snsc.stop(stopSparkContext, stopGracefully)
  }

  def sql(sqlText: String): Dataset[Row] = snsc.sql(sqlText)

  /**
   * Registers and executes given SQL query and
   * returns [[SchemaDStream]] to consume the results
   * @param queryStr
   * @return
   */
  def registerCQ(queryStr: String): SchemaDStream = snsc.registerCQ(queryStr)

  /**
   * Registers and executes given SQL query and
   * returns [[SchemaDStream]] to consume the results
   * @param queryStr
   */
 def registerPythonCQ(queryStr: String): JavaDStream[Row] = {
    val dstream = registerCQ(queryStr)
    JavaDStream.fromDStream(dstream)
  }

  def createSchemaDStream(rowStream: JavaDStream[_], beanClass: Class[_]
  ): SchemaDStream = {
    StreamSqlHelper.createSchemaDStream(snsc, rowStream, beanClass)
  }

  def getSchemaDStream(tableName: String): SchemaDStream = {
    StreamSqlHelper.getSchemaDStream(snsc, tableName)
  }

}
object JavaSnappyStreamingContext {

  private def jcontextOptionToOptional[T](option: Option[SnappyStreamingContext]
      ): Optional[JavaSnappyStreamingContext] =
    option match {
      case Some(value) => Optional.of(new JavaSnappyStreamingContext(value))
      case None => Optional.absent()
    }

  /**
   * :: Experimental ::
   *
   * Get the currently created context, it may be started or not, but never stopped.
   */
  @Experimental
  def getInstance(): Optional[JavaSnappyStreamingContext] = {
    jcontextOptionToOptional(SnappyStreamingContext.getInstance())
  }


  /**
   * :: Experimental ::   *
   * Get the currently active context, if there is one. Active means started but not stopped.
   */
  @Experimental
  def getActive(): Optional[JavaSnappyStreamingContext] = {
    jcontextOptionToOptional(SnappyStreamingContext.getActive)
  }

  /**
   * :: Experimental ::
   *
   * Either return the "active" JavaSnappyStreamingContext (that is, started but not stopped),
   * or create a new JavaSnappyStreamingContext that is
   * @param creatingFunc   Function to create a new StreamingContext
   */
  @Experimental
  def getActiveOrCreate(creatingFunc: JFunction0[JavaSnappyStreamingContext]
      ): JavaSnappyStreamingContext = {
    val snsc = SnappyStreamingContext.getActiveOrCreate(() => creatingFunc.call().snsc)
    new JavaSnappyStreamingContext(snsc)
  }

  /**
   * :: Experimental ::
   *
   * Either get the currently active JavaSnappyStreamingContext (that is, started but
   * not stopped), OR recreate a JavaSnappyStreamingContext from checkpoint data in
   * the given path. If checkpoint data does not exist in the provided, then create
   * a new JavaSnappyStreamingContext by calling the provided
   * `creatingFunc`.
   *
   * @param checkpointPath Checkpoint directory used in an earlier JavaSnappyStreamingContext
   *                       program
   * @param creatingFunc   Function to create a new JavaSnappyStreamingContext
   * @param hadoopConf     Optional Hadoop configuration if necessary for reading from the
   *                       file system
   * @param createOnError  Optional, whether to create a new JavaSnappyStreamingContext if there
   *                       is an error in reading checkpoint data. By default, an exception will
   *                       be thrown on error.
   */
  @Experimental
  def getActiveOrCreate(
      checkpointPath: String,
      creatingFunc: JFunction0[JavaSnappyStreamingContext],
      hadoopConf: Configuration = SparkHadoopUtil.get.conf,
      createOnError: Boolean = false
      ): JavaSnappyStreamingContext = {

    val snsc = SnappyStreamingContext.getActiveOrCreate(checkpointPath
      , () => creatingFunc.call().snsc, hadoopConf
      , createOnError)

    new JavaSnappyStreamingContext(snsc)
  }

  /**
   * Either recreate a JavaSnappyStreamingContext from checkpoint data or create a new
   * JavaSnappyStreamingContext.
   * If checkpoint data exists in the provided `checkpointPath`, then JavaSnappyStreamingContext
   * will be recreated from the checkpoint data. If the data does not exist, then the
   * JavaSnappyStreamingContext will be created by called the provided `creatingFunc`.
   *
   * @param checkpointPath Checkpoint directory used in an earlier JavaSnappyStreamingContext
   *                       program
   * @param creatingFunc   Function to create a new JavaSnappyStreamingContext
   * @param hadoopConf     Optional Hadoop configuration if necessary for reading from the
   *                       file system
   * @param createOnError  Optional, whether to create a new JavaSnappyStreamingContext if there
   *                       is an error in reading checkpoint data. By default, an exception will
   *                       be thrown on error.
   */

  def getOrCreate(
      checkpointPath: String,
      creatingFunc: JFunction0[JavaSnappyStreamingContext],
      hadoopConf: Configuration = SparkHadoopUtil.get.conf,
      createOnError: Boolean = false
      ): JavaSnappyStreamingContext = {
    val snsc = SnappyStreamingContext.getOrCreate(
      checkpointPath, () => creatingFunc.call().snsc, hadoopConf,
      createOnError)

    new JavaSnappyStreamingContext(snsc)
  }
}
