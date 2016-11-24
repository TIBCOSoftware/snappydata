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
package io.snappydata.gemxd

import java.io.DataOutput

import scala.collection.JavaConverters._

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.internal.shared.Version
import com.gemstone.gemfire.internal.{ByteArrayDataInput, InternalDataSerializer}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.message.LeadNodeExecutorMsg
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.distributed.{GfxdHeapDataOutputStream, SnappyResultHolder}
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor
import com.pivotal.gemfirexd.internal.impl.sql.execute.ValueRow
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import com.pivotal.gemfirexd.internal.snappy.{LeadNodeExecutionContext, SparkSQLExecute}
import io.snappydata.{Constant, QueryHint}

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{CachedDataFrame, SnappyContext, SnappySession}
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.util.SnappyUtils
import org.apache.spark.{Logging, SparkContext, SparkEnv}

/**
 * Encapsulates a Spark execution for use in query routing from JDBC.
 */
class SparkSQLExecuteImpl(val sql: String,
    val schema: String,
    val ctx: LeadNodeExecutionContext,
    senderVersion: Version) extends SparkSQLExecute with Logging {

  // spark context will be constructed by now as this will be invoked when
  // DRDA queries will reach the lead node

  if (Thread.currentThread().getContextClassLoader != null) {
    val loader = SnappyUtils.getSnappyStoreContextLoader(getContextOrCurrentClassLoader)
    Thread.currentThread().setContextClassLoader(loader)
  }

  private[this] val session = SnappySessionPerConnection
      .getSnappySessionForConnection(ctx.getConnId)

  session.setSchema(schema)

  private[this] val df = session.sql(sql)

  private[this] val thresholdListener = Misc.getMemStore.thresholdListener()

  private[this] val hdos = new GfxdHeapDataOutputStream(
    thresholdListener, sql, true, senderVersion)

  private[this] val querySchema = df.schema

  private[this] lazy val colTypes = getColumnTypes

  // check for query hint to serialize complex types as JSON strings
  private[this] val complexTypeAsJson = session.getPreviousQueryHints.get(
    QueryHint.ComplexTypeAsJson.toString) match {
    case Some(v) => Misc.parseBoolean(v)
    case None => false
  }

  private val (allAsClob, columnsAsClob) = session.getPreviousQueryHints.get(
    QueryHint.ColumnsAsClob.toString) match {
    case Some(v) => Utils.parseColumnsAsClob(v)
    case None => (false, Set.empty[String])
  }

  private def handleLocalExecution(srh: SnappyResultHolder,
      size: Int): Unit = {
    // prepare SnappyResultHolder with all data and create new one
    if (size > 0) {
      val bytes = new Array[Byte](size + 1)
      // byte 1 will indicate that the metainfo is being packed too
      bytes(0) = if (srh.hasMetadata) 0x1 else 0x0
      hdos.sendTo(bytes, 1)
      srh.fromSerializedData(bytes, bytes.length, null)
    }
  }

  override def packRows(msg: LeadNodeExecutorMsg,
      snappyResultHolder: SnappyResultHolder): Unit = {

    var srh = snappyResultHolder
    val isLocalExecution = msg.isLocallyExecuted

    val bm = SparkEnv.get.blockManager
    val rddId = df.rddId
    var blockReadSuccess = false
    try {
      // get the results and put those in block manager to avoid going OOM
      // TODO: can optimize to ship immediately if plan is not ordered
      // TODO: can ship CollectAggregateExec processing to the server node
      // which is supported vial the "skipLocalCollectProcessing" flag to the
      // call below (but that has additional overheads of plan
      //   shipping/compilation etc and lack of proper BlockManager usage in
      //   messaging + server-side final processing, so do it selectively)
      val partitionBlocks = df.collectWithHandler(CachedDataFrame,
        CachedDataFrame.localBlockStoreResultHandler(rddId, bm),
        CachedDataFrame.localBlockStoreDecoder(querySchema.length, bm))
      hdos.clearForReuse()
      writeMetaData(srh)

      var id = 0
      for (block <- partitionBlocks) {
        block match {
          case null => // skip but still id has to be incremented
          case data: Array[Byte] => if (data.length > 0) {
            hdos.write(data)
          }
          case p: RDDBlockId =>
            val partitionData = Utils.getPartitionData(p, bm)
            // remove the block once a local handle to it has been obtained
            bm.removeBlock(p, tellMaster = false)
            hdos.write(partitionData)
        }
        logTrace(s"Writing data for partition ID = $id: $block")
        val dosSize = hdos.size()
        if (dosSize > GemFireXDUtils.DML_MAX_CHUNK_SIZE) {
          if (isLocalExecution) {
            // prepare SnappyResultHolder with all data and create new one
            handleLocalExecution(srh, dosSize)
            msg.sendResult(srh)
            srh = new SnappyResultHolder(this)
          } else {
            // throttle sending if target node is CRITICAL_UP
            val targetMember = msg.getSender
            if (thresholdListener.isCritical ||
                thresholdListener.isCriticalUp(targetMember)) {
              try {
                var throttle = true
                for (tries <- 1 to 5 if throttle) {
                  Thread.sleep(4)
                  throttle = thresholdListener.isCritical ||
                      thresholdListener.isCriticalUp(targetMember)
                }
              } catch {
                case ie: InterruptedException => Misc.checkIfCacheClosing(ie)
              }
            }

            msg.sendResult(srh)
          }
          logTrace(s"Sent one batch for result, current partition ID = $id")
          hdos.clearForReuse()
          // 0/1 indicator is now written in serializeRows itself to allow
          // ByteBuffer to be passed as is in the chunks list of
          // GfxdHeapDataOutputStream and avoid a copy
        }
        id += 1
      }
      blockReadSuccess = true

      if (isLocalExecution) {
        handleLocalExecution(srh, hdos.size())
      }
      msg.lastResult(srh)

    } finally {
      if (!blockReadSuccess) {
        // remove any cached results from block manager
        bm.removeRdd(rddId)
      }
    }
  }

  override def serializeRows(out: DataOutput, hasMetadata: Boolean): Unit = {
    val numBytes = hdos.size
    if (numBytes > 0) {
      InternalDataSerializer.writeArrayLength(numBytes + 1, out)
      // byte 1 will indicate that the metainfo is being packed too
      out.writeByte(if (hasMetadata) 0x1 else 0x0)
      hdos.sendTo(out)
    } else {
      InternalDataSerializer.writeArrayLength(0, out)
    }
  }

  private lazy val (tableNames, nullability) = getTableNamesAndNullability

  def getTableNamesAndNullability: (Array[String], Array[Boolean]) = {
    var i = 0
    val output = df.queryExecution.analyzed.output
    val tables = new Array[String](output.length)
    val nullables = new Array[Boolean](output.length)
    output.foreach { a =>
      val fn = a.qualifiedName
      val dotIdx = fn.lastIndexOf('.')
      if (dotIdx > 0) {
        tables(i) = fn.substring(0, dotIdx)
      } else {
        tables(i) = ""
      }
      nullables(i) = a.nullable
      i += 1
    }
    (tables, nullables)
  }

  private def writeMetaData(srh: SnappyResultHolder): Unit = {
    val hdos = this.hdos
    // indicates that the metainfo is being packed too
    srh.setHasMetadata()
    DataSerializer.writeStringArray(tableNames, hdos)
    DataSerializer.writeStringArray(getColumnNames, hdos)
    DataSerializer.writeBooleanArray(nullability, hdos)
    colTypes.foreach { case (tp, precision, scale) =>
      InternalDataSerializer.writeSignedVL(tp, hdos)
      if (tp == StoredFormatIds.SQL_DECIMAL_ID) {
        InternalDataSerializer.writeSignedVL(precision, hdos) // precision
        InternalDataSerializer.writeSignedVL(scale, hdos) // scale
      } else if (tp == StoredFormatIds.SQL_VARCHAR_ID ||
          tp == StoredFormatIds.SQL_CHAR_ID) {
        // Write the size as precision
        InternalDataSerializer.writeSignedVL(precision, hdos)
      }
    }
  }

  def getColumnNames: Array[String] = {
    querySchema.fieldNames
  }

  private def getColumnTypes: Array[(Int, Int, Int)] =
    querySchema.map(f => getSQLType(f)).toArray

  private def getSQLType(f: StructField): (Int, Int, Int) = {
    val dataType = f.dataType
    dataType match {
      case IntegerType => (StoredFormatIds.SQL_INTEGER_ID, -1, -1)
      case StringType =>
        TypeUtils.getMetadata[Long](Constant.CHAR_TYPE_SIZE_PROP,
          f.metadata) match {
          case Some(s) =>
            val size = s.asInstanceOf[Int]
            val base = f.metadata.getString(Constant.CHAR_TYPE_BASE_PROP)
            if (allAsClob ||
                (columnsAsClob.nonEmpty && columnsAsClob.contains(f.name))) {
              if (base != "STRING") {
                if (base == "VARCHAR") {
                  (StoredFormatIds.SQL_VARCHAR_ID, size, -1)
                } else {
                  // CHAR
                  (StoredFormatIds.SQL_CHAR_ID, size, -1)
                }
              } else {
                // STRING and CLOB
                (StoredFormatIds.SQL_CLOB_ID, -1, -1)
              }
            } else {
              if (base == "CHAR") {
                (StoredFormatIds.SQL_CHAR_ID, size, -1)
              } else {
                // VARCHAR and STRING
                if (!SparkSQLExecuteImpl.STRING_AS_CLOB ||
                    size < Constant.MAX_VARCHAR_SIZE) {
                  (StoredFormatIds.SQL_VARCHAR_ID, size, -1)
                }
                else {
                  (StoredFormatIds.SQL_CLOB_ID, -1, -1)
                }
              }
            }
          case _ => (StoredFormatIds.SQL_CLOB_ID, -1, -1) // CLOB
        }
      case LongType => (StoredFormatIds.SQL_LONGINT_ID, -1, -1)
      case TimestampType => (StoredFormatIds.SQL_TIMESTAMP_ID, -1, -1)
      case DateType => (StoredFormatIds.SQL_DATE_ID, -1, -1)
      case DoubleType => (StoredFormatIds.SQL_DOUBLE_ID, -1, -1)
      case t: DecimalType => (StoredFormatIds.SQL_DECIMAL_ID,
          t.precision, t.scale)
      case FloatType => (StoredFormatIds.SQL_REAL_ID, -1, -1)
      case BooleanType => (StoredFormatIds.SQL_BOOLEAN_ID, -1, -1)
      case ShortType => (StoredFormatIds.SQL_SMALLINT_ID, -1, -1)
      case ByteType => (StoredFormatIds.SQL_TINYINT_ID, -1, -1)
      case BinaryType => (StoredFormatIds.SQL_BLOB_ID, -1, -1)
      case _: ArrayType | _: MapType | _: StructType =>
        // indicates complex types serialized as strings
        if (complexTypeAsJson) (StoredFormatIds.REF_TYPE_ID, -1, -1)
        else (StoredFormatIds.SQL_BLOB_ID, -1, -1)

      // send across rest as objects that will be displayed as strings
      case _ => (StoredFormatIds.SQL_USERTYPE_ID_V3, -1, -1)
    }
  }

  def getContextOrCurrentClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader)
        .getOrElse(getClass.getClassLoader)
}

object SparkSQLExecuteImpl extends Logging {

  lazy val STRING_AS_CLOB = System.getProperty(Constant.STRING_AS_CLOB_PROP,
    "false").toBoolean

  def getRowIterator(dvds: Array[DataValueDescriptor], types: Array[Int],
      precisions: Array[Int], scales: Array[Int],
      input: ByteArrayDataInput): java.util.Iterator[ValueRow] = {
    val execRow = new ValueRow(dvds)
    val numFields = types.length
    val unsafeRows = CachedDataFrame.decodeUnsafeRows(numFields,
      input.array(), input.position(), input.available())
    unsafeRows.map { row =>
      var index = 0
      while (index < numFields) {
        val dvd = dvds(index)
        if (row.isNullAt(index)) {
          dvd.setToNull()
          index += 1
        } else {
          types(index) match {
            case StoredFormatIds.SQL_CHAR_ID |
                 StoredFormatIds.SQL_VARCHAR_ID |
                 StoredFormatIds.SQL_CLOB_ID =>
              val utf8String = row.getUTF8String(index)
              dvd.setValue(utf8String.toString)

            case StoredFormatIds.SQL_INTEGER_ID =>
              dvd.setValue(row.getInt(index))
            case StoredFormatIds.SQL_LONGINT_ID =>
              dvd.setValue(row.getLong(index))
            case StoredFormatIds.SQL_SMALLINT_ID =>
              dvd.setValue(row.getShort(index))

            case StoredFormatIds.SQL_TIMESTAMP_ID =>
              val ts = DateTimeUtils.toJavaTimestamp(row.getLong(index))
              dvd.setValue(ts)
            case StoredFormatIds.SQL_DECIMAL_ID =>
              val dec = row.getDecimal(index, precisions(index), scales(index))
              dvd.setBigDecimal(dec.toJavaBigDecimal)
            case StoredFormatIds.SQL_DATE_ID =>
              val dt = DateTimeUtils.toJavaDate(row.getInt(index))
              dvd.setValue(dt)
            case StoredFormatIds.SQL_BOOLEAN_ID =>
              dvd.setValue(row.getBoolean(index))
            case StoredFormatIds.SQL_TINYINT_ID =>
              dvd.setValue(row.getByte(index))
            case StoredFormatIds.SQL_REAL_ID =>
              dvd.setValue(row.getFloat(index))
            case StoredFormatIds.SQL_DOUBLE_ID =>
              dvd.setValue(row.getDouble(index))
            case StoredFormatIds.REF_TYPE_ID =>
              // convert to String and write (need DataType)
              /*
              val sb = new StringBuilder()
              Utils.dataTypeStringBuilder(dataType, sb)(row.get(index, dataType))
              */
              throw new GemFireXDRuntimeException("SW: implement")
            case StoredFormatIds.SQL_USERTYPE_ID_V3 =>
              throw new GemFireXDRuntimeException("SW: implement")
            case StoredFormatIds.SQL_BLOB_ID =>
              // all complex types too work with below because all of
              // Array, Map, Struct (as well as Binary itself) store data
              // in the same way in UnsafeRow (offsetAndWidth)
              dvd.setValue(row.getBinary(index))
            case other => throw new GemFireXDRuntimeException(
              s"SparkSQLExecuteImpl: unexpected typeFormatId $other")
          }
          index += 1
        }
      }
      execRow
    }.asJava
  }
}

object SnappySessionPerConnection {

  private val connectionIdMap =
    new java.util.concurrent.ConcurrentHashMap[java.lang.Long, SnappySession]()

  def getSnappySessionForConnection(connId: Long): SnappySession = {
    val connectionID = Long.box(connId)
    val context = connectionIdMap.get(connectionID)
    if (context != null) context
    else {
      val session = SnappyContext(null: SparkContext).snappySession
      val oldContext = connectionIdMap.putIfAbsent(connectionID, session)
      if (oldContext == null) session else oldContext
    }
  }

  def removeSnappySession(connectionID: java.lang.Long): Unit = {
    connectionIdMap.remove(connectionID)
  }
}
