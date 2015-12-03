package io.snappydata.gemxd

import java.io.{DataInput, DataOutput}

import scala.collection.mutable.ListBuffer

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.internal.InternalDataSerializer
import com.gemstone.gemfire.internal.shared.Version
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.message.LeadNodeExecutorMsg
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.distributed.{ActiveColumnBits, GfxdHeapDataOutputStream, SnappyResultHolder}
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import com.pivotal.gemfirexd.internal.snappy.{LeadNodeExecutionContext, SparkSQLExecute}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SnappyContext}
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.{Logging, SparkEnv}

/**
 * Created by kneeraj on 20/10/15.
 */
class SparkSQLExecuteImpl(val sql: String, val ctx: LeadNodeExecutionContext,
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
    val partitionIdList = new ListBuffer[Int]()
    //get the results and put those in block manager to avoid going OOM
    snx.runJob(resultsRdd, (iter: Iterator[InternalRow]) => iter.toArray,
      (partitionId, arr: Array[InternalRow]) => {
        if (arr.length > 0) {
          bm.putSingle(RDDBlockId(resultsRdd.id, partitionId), arr, StorageLevel.MEMORY_AND_DISK,
            false)
          partitionIdList.+=(partitionId)
        }
      })

    hdos.clearForReuse()
    partitionIdList.sorted.foreach(p => {
//      logTrace("Sending data for partition id = " + p)
      val partitionData: Array[InternalRow] = bm.getLocal(RDDBlockId(resultsRdd.id, p)) match {
        case Some(block) => block.data.next().asInstanceOf[Array[InternalRow]]
     // case None => throw new Exception(s"SparkSQLExecuteImpl: packRows() block $RDDBlockId not found")
      }

      var numRowsSentInBatch = 0
      var totalRowsSentForPartition = 0
      while (totalRowsSentForPartition < partitionData.length) {
        partitionData.takeWhile(_ => hdos.size <= GemFireXDUtils.DML_MAX_CHUNK_SIZE).foreach(row =>
        {
          if (numRowsSentInBatch == 0) sendMetaData()
          writeRow(row)

          numRowsSentInBatch += 1
        })
        msg.sendResult(snappyResultHolder)
        logTrace(s"Sent one batch for partition $p. No of rows sent in batch = $numRowsSentInBatch" )
        totalRowsSentForPartition += numRowsSentInBatch
        numRowsSentInBatch = 0;
        hdos.clearForReuse()
      }

      logTrace(s"Finished sending data for partition $p")
    })

    //TODO: could we avoid sending metadata for dummy last result
    hdos.clearForReuse();
    sendMetaData();
    msg.lastResult(snappyResultHolder)

    // remove cached results from block manager
    val numBlocksRemoved = bm.removeRdd(resultsRdd.id)
    assert(numBlocksRemoved == partitionIdList.size)
  }

  private lazy val (tableNames, nullability) = getTableNamesAndNullability()

  def getTableNamesAndNullability():(Array[String], Array[Boolean])= {
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

  def sendMetaData(): Unit = {
    // byte 1 will indicate that the metainfo is being packed too
    hdos.writeByte(0x01);
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
  private def getNumColumnGroups(row: InternalRow) = {
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
      getNumColumnGroups(r)
    }
    while (groupNum < numEightColGroups - 1) {
      writeAGroup(groupNum, 8, r, hdos)
      groupNum += 1;
    }
    writeAGroup(groupNum, numPartCols, r, hdos)
  }

  private def writeAGroup(groupNum: Int, numColsInGrp: Int, row: InternalRow, dos: DataOutput) = {
    var activeByteForGroup: Byte = 0x00
    var colIndex: Int = 0
    var index: Int = 0
    while (index < numColsInGrp) {
      colIndex = (groupNum << 3) + index
      if (!row.isNullAt(colIndex)) {
        activeByteForGroup = ActiveColumnBits.setFlagForNormalizedColumnPosition(index,
          activeByteForGroup)
      }
      index += 1;
    }
    DataSerializer.writePrimitiveByte(activeByteForGroup, dos)
    index = 0
    while (index < numColsInGrp) {
      colIndex = (groupNum << 3) + index
      if (ActiveColumnBits.isNormalizedColumnOn(index, activeByteForGroup)) {
        writeColDataInOptimizedWay(row, colIndex, hdos)
      }
      index += 1;
    }
  }

  private def writeColDataInOptimizedWay(row: InternalRow, colIndex: Int, out: DataOutput) = {
    val sf = schema(colIndex)
    sf.dataType match {
      case TimestampType => InternalDataSerializer.writeSignedVL(row.getLong(colIndex), hdos)
      case BooleanType => hdos.writeBoolean(row.getBoolean(colIndex))
      case DateType => InternalDataSerializer.writeSignedVL(row.getLong(colIndex), hdos)
      case LongType => InternalDataSerializer.writeSignedVL(row.getLong(colIndex), hdos)
      case ShortType => InternalDataSerializer.writeSignedVL(row.getInt(colIndex), hdos)
      case ByteType => DataSerializer.writePrimitiveByte(row.getByte(colIndex), hdos)
      case IntegerType => InternalDataSerializer.writeSignedVL(row.getInt(colIndex), hdos)
      case t: DecimalType => DataSerializer.writeObject(row.getDecimal(colIndex, t.precision,
        t.scale), hdos)
      case FloatType => hdos.writeFloat(row.getFloat(colIndex))
      case DoubleType => hdos.writeDouble(row.getDouble(colIndex))
      case StringType => DataSerializer.writeString(row.getString(colIndex), hdos)
      // TODO: KN add varchar when that data type is identified
      // case VarCharType => StoredFormatIds.SQL_VARCHAR_ID
    }
  }
}

object SparkSQLExecuteImpl {
  def unpackRows(in: DataInput) = {

  }
}


