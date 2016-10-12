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
import java.nio.charset.StandardCharsets

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.internal.shared.Version
import com.gemstone.gemfire.internal.{ByteArrayDataInput, InternalDataSerializer}
import com.gemstone.gnu.trove.TIntObjectHashMap
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.message.LeadNodeExecutorMsg
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.distributed.{ActiveColumnBits, GfxdHeapDataOutputStream, SnappyResultHolder}
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import com.pivotal.gemfirexd.internal.snappy.{LeadNodeExecutionContext, SparkSQLExecute}
import io.snappydata.{Constant, QueryHint}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SnappyContext}
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
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

  private[this] val snc = SnappyContextPerConnection
      .getSnappyContextForConnection(ctx.getConnId)

  snc.setSchema(schema)

  private[this] val df = snc.sql(sql)

  private[this] val hdos = new GfxdHeapDataOutputStream(
    Misc.getMemStore.thresholdListener(), sql, true, senderVersion)

  private[this] val querySchema = df.schema

  private[this] val resultsRdd = df.queryExecution.toRdd

  private[this] lazy val colTypes = getColumnTypes

  // check for query hint to serialize complex types as CLOBs
  private[this] val complexTypeAsClob = snc.snappySession.getPreviousQueryHints.get(
    QueryHint.ComplexTypeAsClob.toString) match {
    case Some(v) => Misc.parseBoolean(v)
    case None => false
  }

  private val (allAsClob, columnsAsClob) = snc.snappySession.getPreviousQueryHints.get(
    QueryHint.ColumnsAsClob.toString) match {
    case Some(v) => Utils.parseColumnsAsClob(v)
    case None => (false, Array.empty[String])
  }

  override def packRows(msg: LeadNodeExecutorMsg,
      snappyResultHolder: SnappyResultHolder): Unit = {

    var srh = snappyResultHolder
    val isLocalExecution = msg.isLocallyExecuted
    val bm = SparkEnv.get.blockManager
    val partitionBlockIds = new Array[RDDBlockId](resultsRdd.partitions.length)
    val serializeComplexType = !complexTypeAsClob && querySchema.exists(
      _.dataType match {
        case _: ArrayType | _: MapType | _: StructType => true
        case _ => false
      })
    val handler = new ExecutionHandler(sql, querySchema, resultsRdd.id,
      partitionBlockIds, serializeComplexType, colTypes)
    var blockReadSuccess = false
    try {
      // get the results and put those in block manager to avoid going OOM
      handler(resultsRdd, df)
      hdos.clearForReuse()
      var metaDataSent = false
      for (p <- partitionBlockIds if p != null) {
        logTrace("Sending data for partition id = " + p)
        val partitionData: ByteBuffer = bm.getLocalBytes(p) match {
          case Some(block) => try {
            block.toByteBuffer
          } finally {
            bm.releaseLock(p)
          }
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
          assert(metaDataSent)
          hdos.writeByte(0x00)
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

  override def serializeRows(out: DataOutput): Unit = {
    val numBytes = hdos.size
    if (numBytes > 0) {
      InternalDataSerializer.writeArrayLength(numBytes, out)
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
      case s: StringType =>
        val hasProp = f.metadata.contains(Constant.CHAR_TYPE_SIZE_PROP)
        lazy val base = f.metadata.getString(Constant.CHAR_TYPE_BASE_PROP)
        lazy val size = f.metadata.getLong(Constant.CHAR_TYPE_SIZE_PROP).asInstanceOf[Int]
        if (allAsClob || columnsAsClob.contains(f.name)) {
            if (hasProp && !base.equals("STRING")) {
              if (base.equals("VARCHAR")) {
                (StoredFormatIds.SQL_VARCHAR_ID, size, -1)
              } else { // CHAR
                (StoredFormatIds.SQL_CHAR_ID, size, -1)
              }
            } else { // STRING and CLOB
              (StoredFormatIds.SQL_CLOB_ID, -1, -1)
            }
        } else if (hasProp) {
          if (base.equals("CHAR")) {
            (StoredFormatIds.SQL_CHAR_ID, size, -1)
          } else { // VARCHAR and STRING
            if ( !SparkSQLExecuteImpl.STRING_AS_CLOB || size < Constant.MAX_VARCHAR_SIZE ) {
              (StoredFormatIds.SQL_VARCHAR_ID, size, -1)
            }
            else {
              (StoredFormatIds.SQL_CLOB_ID, -1, -1)
            }
          }
        } else { // CLOB
          (StoredFormatIds.SQL_CLOB_ID, -1, -1)
        }
      case BinaryType => (StoredFormatIds.SQL_BLOB_ID, -1, -1)
      case _: ArrayType | _: MapType | _: StructType =>
        // the ID here is different from CLOB because serialization of CLOB
        // uses full UTF8 like in UTF8String while below is still modified
        // UTF8 (no code for full UTF8 yet -- change when full UTF8 code added)
        if (complexTypeAsClob) (StoredFormatIds.REF_TYPE_ID, -1, -1)
        else (StoredFormatIds.SQL_BLOB_ID, -1, -1)
      // TODO: KN add varchar when that data type is identified
      // case VarCharType => StoredFormatIds.SQL_VARCHAR_ID

      // send across rest as CLOBs
      case _ => (StoredFormatIds.REF_TYPE_ID, -1, -1)
    }
  }

  def getContextOrCurrentClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
}

object SparkSQLExecuteImpl {

  lazy val STRING_AS_CLOB = System.getProperty(Constant.STRING_AS_CLOB_PROP, "false").toBoolean

  def writeRow(row: InternalRow, numCols: Int, numEightColGroups: Int,
      numPartCols: Int, schema: StructType, hdos: GfxdHeapDataOutputStream,
      bufferHolders: TIntObjectHashMap, rowStoreColTypes: Array[(Int, Int, Int)] = null): Unit = {
    var groupNum: Int = 0
    // using the gemfirexd way of sending results where in the number of
    // columns in each row is divided into sets of 8 columns. Per eight column group a
    // byte will be sent to indicate which all column in that group has a
    // non-null value.
    while (groupNum < numEightColGroups) {
      writeAGroup(groupNum, 8, row, schema, hdos, bufferHolders, rowStoreColTypes)
      groupNum += 1
    }
    writeAGroup(groupNum, numPartCols, row, schema, hdos, bufferHolders, rowStoreColTypes)
  }

  private def writeAGroup(groupNum: Int, numColsInGrp: Int, row: InternalRow,
      schema: StructType, hdos: GfxdHeapDataOutputStream,
      bufferHolders: TIntObjectHashMap, rowStoreColTypes: Array[(Int, Int, Int)] = null): Unit = {
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
        writeColDataInOptimizedWay(row, colIndex, schema, hdos, bufferHolders, rowStoreColTypes)
      }
      index += 1
    }
  }

  private def writeStringColumnAsPerFormatId(row: InternalRow, colIndex: Int,
      hdos: GfxdHeapDataOutputStream, rowStoreColTypes: Array[(Int, Int, Int)] = null): Unit = {
    if (rowStoreColTypes != null && rowStoreColTypes(colIndex)._1 == StoredFormatIds.SQL_CLOB_ID) {
      val utf8String = row.getUTF8String(colIndex)
      if (utf8String ne null) {
        val utfLen = utf8String.numBytes()
        InternalDataSerializer.writeSignedVL(utfLen, hdos)
        hdos.copyMemory(utf8String.getBaseObject,
          utf8String.getBaseOffset, utfLen)
      } else {
        InternalDataSerializer.writeSignedVL(-1, hdos)
      }
    }
    else {
      // null value won't come here
      DataSerializer.writeString(row.getString(colIndex), hdos)
    }
  }

  private def writeColDataInOptimizedWay(row: InternalRow, colIndex: Int,
      schema: StructType, hdos: GfxdHeapDataOutputStream,
      bufferHolders: TIntObjectHashMap, rowStoreColTypes: Array[(Int, Int, Int)] = null): Unit = {
    schema(colIndex).dataType match {
      case StringType =>
        writeStringColumnAsPerFormatId(row, colIndex, hdos, rowStoreColTypes)
      case IntegerType =>
        InternalDataSerializer.writeSignedVL(row.getInt(colIndex), hdos)
      case LongType =>
        InternalDataSerializer.writeSignedVL(row.getLong(colIndex), hdos)
      case TimestampType =>
        InternalDataSerializer.writeSignedVL(row.getLong(colIndex), hdos)
      case t: DecimalType => DataSerializer.writeObject(row.getDecimal(
        colIndex, t.precision, t.scale).toJavaBigDecimal, hdos)
      case BooleanType => hdos.writeBoolean(row.getBoolean(colIndex))
      case DateType =>
        InternalDataSerializer.writeSignedVL(row.getInt(colIndex), hdos)
      case ShortType =>
        InternalDataSerializer.writeSignedVL(row.getShort(colIndex), hdos)
      case ByteType => hdos.writeByte(row.getByte(colIndex))
      case FloatType => hdos.writeFloat(row.getFloat(colIndex))
      case DoubleType => hdos.writeDouble(row.getDouble(colIndex))
      case BinaryType => DataSerializer.writeByteArray(
        row.getBinary(colIndex), hdos)
      case a: ArrayType if bufferHolders != null =>
        val buffer = bufferHolders.get(0).asInstanceOf[BufferHolder]
        buffer.cursor = Platform.BYTE_ARRAY_OFFSET
        val serializer = CodeGeneration.getComplexTypeSerializer(a)
        serializer.serialize(row.getArray(colIndex), buffer, hdos)
      case m: MapType if bufferHolders != null =>
        val buffer = bufferHolders.get(0).asInstanceOf[BufferHolder]
        buffer.cursor = Platform.BYTE_ARRAY_OFFSET
        val serializer = CodeGeneration.getComplexTypeSerializer(m)
        serializer.serialize(row.getMap(colIndex), buffer, hdos)
      case s: StructType if bufferHolders != null =>
        val nFields = s.length
        val buffer = bufferHolders.get(nFields).asInstanceOf[BufferHolder]
        buffer.cursor = Platform.BYTE_ARRAY_OFFSET
        val serializer = CodeGeneration.getComplexTypeSerializer(s)
        serializer.serialize(row.getStruct(colIndex, nFields), buffer, hdos)
      case other =>
        val col = row.get(colIndex, other)
        if (col ne null) {
          val sb = new StringBuilder()
          Utils.dataTypeStringBuilder(other, sb)(col)
          // write the full length as an integer
          hdos.writeUTF(sb.toString(), true, false)
        } else {
          hdos.writeInt(-1)
        }
    }
  }

  def readDVDArray(dvds: Array[DataValueDescriptor], types: Array[Int],
      in: ByteArrayDataInput, numEightColGroups: Int,
      numPartialCols: Int): Unit = {
    var groupNum = 0
    // using the gemfirexd way of sending results where in the number of
    // columns in each row is divided into sets of 8 columns. Per eight column
    // group a byte will be sent to indicate which all column in that group
    // has a non-null value.
    while (groupNum < numEightColGroups) {
      readAGroup(groupNum, 8, dvds, types, in)
      groupNum += 1
    }
    readAGroup(groupNum, numPartialCols, dvds, types, in)
  }

  private def readAGroup(groupNum: Int, numColsInGroup: Int,
      dvds: Array[DataValueDescriptor], types: Array[Int],
      in: ByteArrayDataInput): Unit = {
    val activeByteForGroup: Byte = DataSerializer.readPrimitiveByte(in)
    var index: Int = 0
    while (index < numColsInGroup) {
      val dvdIndex = (groupNum << 3) + index
      val dvd = dvds(dvdIndex)
      if (ActiveColumnBits.isNormalizedColumnOn(index, activeByteForGroup)) {
        types(dvdIndex) match {
          case StoredFormatIds.SQL_CLOB_ID =>
            val utfLen = InternalDataSerializer.readSignedVL(in).toInt
            if (utfLen >= 0) {
              val pos = in.position()
              dvd.setValue(new String(in.array(), pos, utfLen,
                StandardCharsets.UTF_8))
              in.setPosition(pos + utfLen)
            } else {
              dvd.setToNull()
            }

          case StoredFormatIds.SQL_INTEGER_ID |
               StoredFormatIds.SQL_LONGINT_ID |
               StoredFormatIds.SQL_SMALLINT_ID =>
            dvd.setValue(InternalDataSerializer.readSignedVL(in))
          case StoredFormatIds.SQL_TIMESTAMP_ID =>
            val ts = DateTimeUtils.toJavaTimestamp(
              InternalDataSerializer.readSignedVL(in))
            dvd.setValue(ts)
          case StoredFormatIds.SQL_DECIMAL_ID =>
            val bd = DataSerializer.readObject[java.math.BigDecimal](in)
            dvd.setBigDecimal(bd)
          case StoredFormatIds.SQL_DATE_ID =>
            val dt = DateTimeUtils.toJavaDate(
              InternalDataSerializer.readSignedVL(in).toInt)
            dvd.setValue(dt)
          case StoredFormatIds.SQL_BOOLEAN_ID =>
            dvd.setValue(in.readBoolean())
          case StoredFormatIds.SQL_TINYINT_ID =>
            dvd.setValue(in.readByte())
          case StoredFormatIds.SQL_REAL_ID =>
            dvd.setValue(in.readFloat())
          case StoredFormatIds.SQL_DOUBLE_ID =>
            dvd.setValue(in.readDouble())
          case StoredFormatIds.SQL_CHAR_ID |
               StoredFormatIds.SQL_VARCHAR_ID =>
            dvd.setValue(DataSerializer.readString(in))
          case StoredFormatIds.REF_TYPE_ID =>
            // read the full length as an integer
            val utfLen = in.readInt()
            if (utfLen >= 0) {
              val pos = in.position()
              dvd.readBytes(in.array(), pos, utfLen)
              in.setPosition(pos + utfLen)
            } else {
              dvd.setToNull()
            }
          case StoredFormatIds.SQL_BLOB_ID =>
            dvd.setValue(DataSerializer.readByteArray(in))
          case other => throw new GemFireXDRuntimeException(
            s"SparkSQLExecuteImpl: unexpected typeFormatId $other")
        }
      } else {
        dvd.setToNull()
      }
      index += 1
    }
  }
}

class ExecutionHandler(sql: String, schema: StructType, rddId: Int,
    partitionBlockIds: Array[RDDBlockId],
    serializeComplexType: Boolean, rowStoreColTypes: Array[(Int, Int, Int)] = null) extends Serializable {

  def apply(resultsRdd: RDD[InternalRow], df: DataFrame): Unit = {
    Utils.withNewExecutionId(df.sparkSession, df.queryExecution) {
      val sc = SnappyContext.globalSparkContext
      sc.runJob(resultsRdd, rowIter _, resultHandler _)
    }
  }

  private[snappydata] def rowIter(itr: Iterator[InternalRow]): Array[Byte] = {

    var numCols = -1
    var numEightColGroups = -1
    var numPartCols = -1

    def evalNumColumnGroups(row: InternalRow): Unit = {
      if (row != null) {
        numCols = row.numFields
        numEightColGroups = numCols / 8
        numPartCols = numCols % 8
      }
    }
    val dos = new GfxdHeapDataOutputStream(
      Misc.getMemStore.thresholdListener(), sql, false, null)
    var bufferHolders: TIntObjectHashMap = null
    if (serializeComplexType) {
      // need to create separate BufferHolders for each of the Structs
      // having different sizes
      schema.foreach(_.dataType match {
        case _: ArrayType | _: MapType =>
          if (bufferHolders == null) {
            bufferHolders = new TIntObjectHashMap(3)
          }
          if (bufferHolders.isEmpty || !bufferHolders.containsKey(0)) {
            bufferHolders.put(0, new BufferHolder(new UnsafeRow()))
          }
        case s: StructType =>
          val nFields = s.length
          if (bufferHolders == null) {
            bufferHolders = new TIntObjectHashMap(3)
          }
          if (bufferHolders.isEmpty || !bufferHolders.containsKey(nFields)) {
            bufferHolders.put(nFields, new BufferHolder(new UnsafeRow(nFields)))
          }
        case _ =>
      })
    }
    itr.foreach { row =>
      if (numCols == -1) {
        evalNumColumnGroups(row)
      }
      SparkSQLExecuteImpl.writeRow(row, numCols, numEightColGroups,
        numPartCols, schema, dos, bufferHolders, rowStoreColTypes)
    }
    dos.toByteArray
  }

  private[snappydata] def resultHandler(partitionId: Int,
      block: Array[Byte]): Unit = {
    if (block.length > 0) {
      val bm = SparkEnv.get.blockManager
      val blockId = RDDBlockId(rddId, partitionId)
      bm.putBytes(blockId, Utils.newChunkedByteBuffer(Array(ByteBuffer.wrap(
        block))), StorageLevel.MEMORY_AND_DISK_SER, tellMaster = false)
      partitionBlockIds(partitionId) = blockId
    }
  }
}

object SnappyContextPerConnection {

  private val connectionIdMap =
    new java.util.concurrent.ConcurrentHashMap[Long, SnappyContext]()

  def getSnappyContextForConnection(connId: Long): SnappyContext = {
    val connectionID = Long.box(connId)
    val context = connectionIdMap.get(connectionID)
    if (context != null) context
    else {
      val snc = SnappyContext(null: SparkContext)
      val oldContext = connectionIdMap.putIfAbsent(connectionID, snc)
      if (oldContext == null) snc else oldContext
    }
  }

  def removeSnappyContext(connectionID: java.lang.Long): Unit = {
    connectionIdMap.remove(connectionID)
  }
}
