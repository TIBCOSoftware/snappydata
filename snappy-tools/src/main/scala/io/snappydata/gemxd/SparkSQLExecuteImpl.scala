package io.snappydata.gemxd

import java.io.DataOutput

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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SnappyContext}
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.{Logging, SparkEnv}

/**
 * Encapsulates a Spark execution for use in query routing from JDBC.
 *
 * Created by kneeraj on 20/10/15.
 */
class SparkSQLExecuteImpl(val sql: String,
    val ctx: LeadNodeExecutionContext,
    senderVersion: Version) extends SparkSQLExecute with Logging {
  // spark context will be constructed by now as this will be invoked when drda queries
  // will reach the lead node
  // TODO: KN Later get the SnappyContext as per the ctx passed to this executor
  private lazy val snx = SnappyContext(null)
  private lazy val df: DataFrame = snx.sql(sql)

  private lazy val hdos = new GfxdHeapDataOutputStream(Misc.getMemStore.thresholdListener(),
    sql, true, senderVersion)

  private lazy val schema = df.schema

  private val resultsRdd = df.queryExecution.executedPlan.execute()

  override def serializeRows(out: DataOutput) = {
    var numBytes = 0
    if (hdos != null && hdos.size > 0) {
      numBytes = hdos.size
      InternalDataSerializer.writeArrayLength(numBytes, out)
      hdos.sendTo(out)
    }
    else {
      InternalDataSerializer.writeArrayLength(numBytes, out)
    }
  }

  override def packRows(msg: LeadNodeExecutorMsg,
      snappyResultHolder: SnappyResultHolder): Unit = {

    val bm = SparkEnv.get.blockManager
    val numPartitions = resultsRdd.partitions.length
    val partitionBlockIds = new Array[RDDBlockId](numPartitions)
    //get the results and put those in block manager to avoid going OOM
    snx.runJob(resultsRdd, (iter: Iterator[InternalRow]) => iter.toArray,
      (partitionId, arr: Array[InternalRow]) => {
        if (arr.length > 0) {
          val blockId = RDDBlockId(resultsRdd.id, partitionId)
          bm.putSingle(blockId, arr, StorageLevel.MEMORY_AND_DISK,
            tellMaster = false)
          partitionBlockIds(partitionId) = blockId
        }
      })

    hdos.clearForReuse()
    var metaDataSent = false
    for (p <- partitionBlockIds if p != null) {
      logTrace("Sending data for partition id = " + p)
      val partitionData: Array[InternalRow] = bm.getLocal(p) match {
        case Some(block) => block.data.next().asInstanceOf[Array[InternalRow]]
        case None => throw new GemFireXDRuntimeException(
          s"SparkSQLExecuteImpl: packRows() block $p not found")
      }

      var numRowsSent = 0
      val totalRowsForPartition = partitionData.length

      while (numRowsSent < totalRowsForPartition) {
        //send metadata once per result set
        if ((numRowsSent == 0) && !metaDataSent) {
          writeMetaData()
          metaDataSent = true
        } else {
          //indicates no metadata being sent
          hdos.writeByte(0x0)
        }
        (numRowsSent until totalRowsForPartition).takeWhile(_ => hdos.size <=
            GemFireXDUtils.DML_MAX_CHUNK_SIZE).foreach(i => {
          val row = partitionData(i)
          writeRow(row)

          numRowsSent += 1
        })

        msg.sendResult(snappyResultHolder)
        logTrace(s"Sent one batch for partition $p. " +
            s"No of rows sent in batch = $numRowsSent")
        hdos.clearForReuse()
      }
      logTrace(s"Finished sending data for partition $p")
    }

    if (!metaDataSent) {
      writeMetaData()
    }
    msg.lastResult(snappyResultHolder)

    // remove cached results from block manager
    bm.removeRdd(resultsRdd.id)
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

  var (numCols, numEightColGroups, numPartCols) = (-1, -1, -1)

  private def evalNumColumnGroups(row: InternalRow): Unit = {
    if (row != null) {
      numCols = row.numFields
      numEightColGroups = (numCols + 7) / 8
      numPartCols = ((numCols - 1) % 8) + 1
    }
  }

  private def writeRow(r: InternalRow) = {
    var groupNum: Int = 0
    // using the gemfirexd way of sending results where in the number of
    // columns in each row is divided into sets of 8 columns. Per eight column group a
    // byte will be sent to indicate which all column in that group has a
    // non-null value.
    if (numCols == -1) {
      evalNumColumnGroups(r)
    }
    while (groupNum < numEightColGroups - 1) {
      writeAGroup(groupNum, 8, r)
      groupNum += 1
    }
    writeAGroup(groupNum, numPartCols, r)
  }

  private def writeAGroup(groupNum: Int, numColsInGrp: Int, row: InternalRow) = {
    var activeByteForGroup: Byte = 0x00
    var colIndex: Int = 0
    var index: Int = 0
    while (index < numColsInGrp) {
      colIndex = (groupNum << 3) + index
      if (!row.isNullAt(colIndex)) {
        activeByteForGroup = ActiveColumnBits.setFlagForNormalizedColumnPosition(index,
          activeByteForGroup)
      }
      index += 1
    }
    DataSerializer.writePrimitiveByte(activeByteForGroup, hdos)
    index = 0
    while (index < numColsInGrp) {
      colIndex = (groupNum << 3) + index
      if (ActiveColumnBits.isNormalizedColumnOn(index, activeByteForGroup)) {
        writeColDataInOptimizedWay(row, colIndex)
      }
      index += 1
    }
  }

  private def writeColDataInOptimizedWay(row: InternalRow, colIndex: Int) = {
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

        val hdos = this.hdos
        if (utf8String ne null) {
          val utfLen = utf8String.numBytes()
          // for length greater than 64K, write a terminating sequence
          if (utfLen > 65535) {
            hdos.writeShort(0)
            hdos.copyMemory(utf8String.getBaseObject,
              utf8String.getBaseOffset, utfLen)
            hdos.writeNoWrap(SparkSQLExecuteImpl.LONG_UTF8_TERMINATION, 0, 3)
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

object SparkSQLExecuteImpl {
  val LONG_UTF8_TERMINATION = Array((0xE0 & 0xFF).toByte, 0.toByte, 0.toByte)
}
