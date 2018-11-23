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
package org.apache.spark.sql.execution.columnar

import java.nio.ByteBuffer
import java.sql.Connection

import scala.util.control.NonFatal

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.types.StructType

trait ExternalStore extends Serializable with Logging {

  final val columnPrefix = "COL_"

  def tableName: String

  def storeColumnBatch(tableName: String, batch: ColumnBatch,
      partitionId: Int, batchId: Long, maxDeltaRows: Int,
      compressionCodecId: Int, conn: Option[Connection]): Unit

  def storeDelete(tableName: String, buffer: ByteBuffer, partitionId: Int,
      batchId: Long, compressionCodecId: Int, conn: Option[Connection]): Unit

  def getColumnBatchRDD(tableName: String, rowBuffer: String, projection: Array[Int],
      filters: Array[Expression], prunePartitions: () => Int, session: SparkSession,
      schema: StructType, delayRollover: Boolean): RDD[Any]

  def getConnectedExternalStore(tableName: String,
      onExecutor: Boolean): ConnectedExternalStore

  def getConnection(id: String, onExecutor: Boolean): java.sql.Connection

  def connProperties: ConnectionProperties

  def tryExecute[T](tableName: String, closeOnSuccessOrFailure: Boolean = true,
      onExecutor: Boolean = false)(f: Connection => T)
      (implicit c: Option[Connection] = None): T = {
    var success = false
    val conn = c.getOrElse(getConnection(tableName, onExecutor))
    try {
      val ret = f(conn)
      success = true
      ret
    } finally {
      if (closeOnSuccessOrFailure && !conn.isInstanceOf[EmbedConnection] && !conn.isClosed) {
        try {
          if (success) conn.commit()
          else handleRollback(conn.rollback)
        } finally {
          conn.close()
        }
      }
    }
  }

  def handleRollback(rollback: () => Unit, finallyCode: () => Unit = null): Unit = {
    try {
      rollback()
    } catch {
      case NonFatal(e) =>
        if (GemFireXDUtils.retryToBeDone(e)) logInfo(e.toString) else logWarning(e.toString, e)
    } finally {
      if (finallyCode ne null) finallyCode()
    }
  }
}

trait ConnectedExternalStore extends ExternalStore {

  private[this] var dependentAction: Option[Connection => Unit] = None

  protected[this] val connectedInstance: Connection

  def conn: Connection = {
    assert(!connectedInstance.isClosed)
    connectedInstance
  }

  def commitAndClose(isSuccess: Boolean): Unit = {
    // ideally shouldn't check for isClosed.it means some bug!
    val conn = connectedInstance
    if (!conn.isInstanceOf[EmbedConnection] && !conn.isClosed) {
      try {
        if (isSuccess) {
          conn.commit()
        } else {
          conn.rollback()
        }
      } finally {
        conn.close()
      }
    }
  }

  override def tryExecute[T](tableName: String,
      closeOnSuccess: Boolean = true, onExecutor: Boolean = false)
      (f: Connection => T)
      (implicit c: Option[Connection]): T = {
    assert(!connectedInstance.isClosed)
    val ret = super.tryExecute(tableName,
      closeOnSuccessOrFailure = false /* responsibility of the user to close later */ ,
      onExecutor)(f)(Some(connectedInstance))

    if (dependentAction.isDefined) {
      assert(!connectedInstance.isClosed)
      dependentAction.get(connectedInstance)
    }

    ret
  }

  def withDependentAction(f: Connection => Unit): ConnectedExternalStore = {
    dependentAction = Some(f)
    this
  }
}
