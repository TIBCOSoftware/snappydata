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
import java.nio.ByteBuffer

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.internal.InternalDataSerializer
import com.gemstone.gemfire.internal.shared.Version
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.message.LeadNodeExecutorMsg
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.distributed.{ActiveColumnBits, GfxdHeapDataOutputStream, SnappyResultHolder}
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import com.pivotal.gemfirexd.internal.snappy.{LeadNodeExecutionContext, SparkSQLExecute}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SnappyUIUtils, DataFrame, SnappyContext}
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.{SparkContext, Logging, SparkEnv}
/**
 * Encapsulates a Spark execution for use in query routing from JDBC.
 */
class SparkSQLExecuteImpl(val sql: String,
    val ctx: LeadNodeExecutionContext,
    senderVersion: Version) extends SparkSQLExecute with Logging {

  // spark context will be constructed by now as this will be invoked when drda queries
  // will reach the lead node
  // TODO: KN Later get the SnappyContext as per the ctx passed to this executor

  private lazy val snx = SnappyContextPerConnection.getSnappyContextForConnection(ctx.getConnId)

  private lazy val df: DataFrame = snx.sql(sql)

  private lazy val hdos = new GfxdHeapDataOutputStream(
    Misc.getMemStore.thresholdListener(), sql, true, senderVersion)

  private lazy val schema = df.schema

  private val resultsRdd = df.queryExecution.executedPlan.execute()

  override def serializeRows(out: DataOutput) = {
    val numBytes = hdos.size
    if (numBytes > 0) {
      InternalDataSerializer.writeArrayLength(numBytes, out)
      hdos.sendTo(out)
    } else {
      InternalDataSerializer.writeArrayLength(0, out)
    }
  }

  override def packRows(msg: LeadNodeExecutorMsg,
      snappyResultHolder: SnappyResultHolder): Unit = {

    var srh = snappyResultHolder
    val isLocalExecution = msg.isLocallyExecuted
    val bm = SparkEnv.get.blockManager
    val partitionBlockIds = new Array[RDDBlockId](resultsRdd.partitions.length)
    val handler = new ExecutionHandler(sql, schema, resultsRdd.id,
      partitionBlockIds)
    var blockReadSuccess = false
    try {
      // get the results and put those in block manager to avoid going OOM
      handler(resultsRdd, df)

      hdos.clearForReuse()
      var metaDataSent = false
      for (p <- partitionBlockIds if p != null) {
        logTrace("Sending data for partition id = " + p)
        val partitionData: ByteBuffer = bm.getLocalBytes(p) match {
          case Some(block) => block
          case None => throw new GemFireXDRuntimeException(
            s"SparkSQLExecuteImpl: packRows() block $p not found")
        }

        if (!metaDataSent) {
          writeMetaData()
          metaDataSent = true
        }
        hdos.write(partitionData.array())
        val dosSize = hdos.size()
        if (dosSize > GemFireXDUtils.DML_MAX_CHUNK_SIZE) {
          if (isLocalExecution) {
            // prepare SnappyResultHolder with all data and create new one
            if (dosSize > 0) {
              val rawData = hdos.toByteArrayCopy
              srh.fromSerializedData(rawData, rawData.length, null)
            }
            msg.sendResult(srh)
            srh = new SnappyResultHolder(this)
          } else {
            msg.sendResult(srh)
          }
          logTrace(s"Sent one batch for result, current partition $p.")
          hdos.clearForReuse()
        }

        // clear persisted block
        bm.removeBlock(p, tellMaster = false)
      }
      blockReadSuccess = true

      if (!metaDataSent) {
        writeMetaData()
      }
      if (isLocalExecution) {
        // prepare SnappyResultHolder with all data and create new one
        if (hdos.size > 0) {
          val rawData = hdos.toByteArrayCopy
          srh.fromSerializedData(rawData, rawData.length, null)
        }
      }
      msg.lastResult(srh)

    } finally {
      if (!blockReadSuccess) {
        // remove cached results from block manager
        bm.removeRdd(resultsRdd.id)
      }
    }
  }

  private lazy val (tableNames, nullability) = getTableNamesAndNullability

  def getTableNamesAndNullability: (Array[String], Array[Boolean]) = {
    var i = 0
    val output = df.queryExecution.analyzed.output
    val tables = new Array[String](output.length)
    val nullables = new Array[Boolean](output.length)
    output.foreach(a => {
      val fn = a.qualifiedName
      val dotIdx = fn.lastIndexOf('.')
      if (dotIdx > 0) {
        tables(i) = fn.substring(0, dotIdx)
      }
      else {
        tables(i) = ""
      }
      nullables(i) = a.nullable
      i = i + 1
    })
    (tables, nullables)
  }

  private def writeMetaData(): Unit = {
    val hdos = this.hdos
    // byte 1 will indicate that the metainfo is being packed too
    hdos.writeByte(0x01)
    DataSerializer.writeStringArray(tableNames, hdos)
    DataSerializer.writeStringArray(getColumnNames, hdos)
    DataSerializer.writeBooleanArray(nullability, hdos)
    val colTypes = getColumnTypes
    colTypes.foreach(x => {
      val t = x._1
      InternalDataSerializer.writeSignedVL(t, hdos)
      if (t == StoredFormatIds.SQL_DECIMAL_ID) {
        InternalDataSerializer.writeSignedVL(x._2, hdos) // precision
        InternalDataSerializer.writeSignedVL(x._3, hdos) // scale
      }
    })
  }

  def getColumnNames: Array[String] = {
    schema.fieldNames
  }

  private def getNumColumns: Int = schema.size

  private def getColumnTypes: Array[(Int, Int, Int)] = {
    val numCols = getNumColumns
    (0 until numCols).map(i => getSQLType(i, schema)).toArray
  }

  private def getSQLType(i: Int, schema: StructType): (Int, Int, Int) = {
    val sf = schema(i)
    sf.dataType match {
      case TimestampType => (StoredFormatIds.SQL_TIMESTAMP_ID, -1, -1)
      case BooleanType => (StoredFormatIds.SQL_BOOLEAN_ID, -1, -1)
      case DateType => (StoredFormatIds.SQL_DATE_ID, -1, -1)
      case LongType => (StoredFormatIds.SQL_LONGINT_ID, -1, -1)
      case ShortType => (StoredFormatIds.SQL_SMALLINT_ID, -1, -1)
      case ByteType => (StoredFormatIds.SQL_TINYINT_ID, -1, -1)
      case IntegerType => (StoredFormatIds.SQL_INTEGER_ID, -1, -1)
      case t: DecimalType => (StoredFormatIds.SQL_DECIMAL_ID, t.precision, t.scale)
      case FloatType => (StoredFormatIds.SQL_REAL_ID, -1, -1)
      case DoubleType => (StoredFormatIds.SQL_DOUBLE_ID, -1, -1)
      case StringType => (StoredFormatIds.SQL_CLOB_ID, -1, -1)
      // TODO: KN add varchar when that data type is identified
      // case VarCharType => StoredFormatIds.SQL_VARCHAR_ID
    }
  }
}

object SparkSQLExecuteImpl {
  val LONG_UTF8_TERMINATION = Array((0xE0 & 0xFF).toByte, 0.toByte, 0.toByte)

  def writeRow(row: InternalRow, numCols: Int, numEightColGroups: Int,
      numPartCols: Int, schema: StructType, hdos: GfxdHeapDataOutputStream) = {
    var groupNum: Int = 0
    // using the gemfirexd way of sending results where in the number of
    // columns in each row is divided into sets of 8 columns. Per eight column group a
    // byte will be sent to indicate which all column in that group has a
    // non-null value.
    while (groupNum < numEightColGroups - 1) {
      writeAGroup(groupNum, 8, row, schema, hdos)
      groupNum += 1
    }
    writeAGroup(groupNum, numPartCols, row, schema, hdos)
  }

  private def writeAGroup(groupNum: Int, numColsInGrp: Int, row: InternalRow,
      schema: StructType, hdos: GfxdHeapDataOutputStream) = {
    var activeByteForGroup: Byte = 0x00
    var colIndex: Int = 0
    var index: Int = 0
    while (index < numColsInGrp) {
      colIndex = (groupNum << 3) + index
      if (!row.isNullAt(colIndex)) {
        activeByteForGroup = ActiveColumnBits
            .setFlagForNormalizedColumnPosition(index, activeByteForGroup)
      }
      index += 1
    }
    DataSerializer.writePrimitiveByte(activeByteForGroup, hdos)
    index = 0
    while (index < numColsInGrp) {
      colIndex = (groupNum << 3) + index
      if (ActiveColumnBits.isNormalizedColumnOn(index, activeByteForGroup)) {
        writeColDataInOptimizedWay(row, colIndex, schema, hdos)
      }
      index += 1
    }
  }

  private def writeColDataInOptimizedWay(row: InternalRow, colIndex: Int,
      schema: StructType, hdos: GfxdHeapDataOutputStream) = {
    schema(colIndex).dataType match {
      case TimestampType => InternalDataSerializer.writeSignedVL(row.getLong(colIndex), hdos)
      case BooleanType => hdos.writeBoolean(row.getBoolean(colIndex))
      case DateType => InternalDataSerializer.writeSignedVL(row.getLong(colIndex), hdos)
      case LongType => InternalDataSerializer.writeSignedVL(row.getLong(colIndex), hdos)
      case ShortType => InternalDataSerializer.writeSignedVL(row.getInt(colIndex), hdos)
      case ByteType => DataSerializer.writePrimitiveByte(row.getByte(colIndex), hdos)
      case IntegerType => InternalDataSerializer.writeSignedVL(row.getInt(colIndex), hdos)
      case t: DecimalType => DataSerializer.writeObject(row.getDecimal(colIndex, t.precision,
        t.scale).toJavaBigDecimal, hdos)
      case FloatType => hdos.writeFloat(row.getFloat(colIndex))
      case DoubleType => hdos.writeDouble(row.getDouble(colIndex))
      case StringType =>
        // keep this consistent with SQLChar.toDataForOptimizedResultHolder
        val utf8String = row.getUTF8String(colIndex)

        if (utf8String ne null) {
          val utfLen = utf8String.numBytes()
          // for length greater than 64K, write a terminating sequence
          if (utfLen > 65535) {
            hdos.writeShort(0)
            hdos.copyMemory(utf8String.getBaseObject,
              utf8String.getBaseOffset, utfLen)
            hdos.writeNoWrap(LONG_UTF8_TERMINATION, 0, 3)
          } else if (utfLen > 0) {
            hdos.writeShort(utfLen)
            hdos.copyMemory(utf8String.getBaseObject,
              utf8String.getBaseOffset, utfLen)
          } else {
            hdos.writeShort(0)
            hdos.writeByte(-1)
          }
        } else {
          hdos.writeShort(0)
          hdos.writeByte(-1)
        }
      // TODO: KN add varchar when that data type is identified
      // case VarCharType => StoredFormatIds.SQL_VARCHAR_ID
    }
  }
}

class ExecutionHandler(sql: String, schema: StructType, rddId: Int,
    partitionBlockIds: Array[RDDBlockId]) extends Logging with Serializable {

  def apply(resultsRdd: RDD[InternalRow], df: DataFrame): Unit = {
    SnappyUIUtils.withNewExecutionId(df.sqlContext, df.queryExecution) {
      val sc = SnappyContext.globalSparkContext
      sc.runJob(resultsRdd, rowIter _, resultHandler _)
    }
  }

  private[snappydata] def rowIter(itr: Iterator[InternalRow]): Array[Byte] = {

    var (numCols, numEightColGroups, numPartCols) = (-1, -1, -1)

    def evalNumColumnGroups(row: InternalRow): Unit = {
      if (row != null) {
        numCols = row.numFields
        numEightColGroups = (numCols + 7) / 8
        numPartCols = ((numCols - 1) % 8) + 1
      }
    }
    val dos = new GfxdHeapDataOutputStream(
      Misc.getMemStore.thresholdListener(), sql, true, null)
    itr.foreach { row =>
      if (numCols == -1) {
        evalNumColumnGroups(row)
      }
      SparkSQLExecuteImpl.writeRow(row, numCols, numEightColGroups,
        numPartCols, schema, dos)
    }
    dos.toByteArray
  }

  private[snappydata] def resultHandler(partitionId: Int,
      block: Array[Byte]): Unit = {
    if (block.length > 0) {
      val bm = SparkEnv.get.blockManager
      val blockId = RDDBlockId(rddId, partitionId)
      bm.putBytes(blockId, ByteBuffer.wrap(block),
        StorageLevel.MEMORY_AND_DISK_SER, tellMaster = false)
      partitionBlockIds(partitionId) = blockId
    }
  }
}

object SnappyContextPerConnection {
  private lazy val concurrentMap = new java.util.concurrent.ConcurrentHashMap[Long, SnappyContext]()

  def getSnappyContextForConnection(connectionID: Long): SnappyContext = {
    if (concurrentMap.get(connectionID) == null) {
      concurrentMap.put(connectionID, SnappyContext.getOrCreate(null).newSession())
    }
    concurrentMap.get(connectionID)
  }

  def removeSnappyContext(connectionID: Long): Unit = {
    concurrentMap.remove(connectionID)
  }
}
