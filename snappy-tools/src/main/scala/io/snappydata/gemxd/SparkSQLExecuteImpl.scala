package io.snappydata.gemxd

import java.io.{DataInput, DataOutput}

import scala.collection.mutable.MutableList

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.internal.InternalDataSerializer
import com.gemstone.gemfire.internal.shared.Version
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.message.LeadNodeExecutorMsg
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.distributed.{ActiveColumnBits, GfxdHeapDataOutputStream, SnappyResultHolder}
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import com.pivotal.gemfirexd.internal.snappy.{LeadNodeExecutionContext, SparkSQLExecute}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SnappyContext}

/**
 * Created by kneeraj on 20/10/15.
 */
class SparkSQLExecuteImpl(val sql: String, val ctx: LeadNodeExecutionContext,
    senderVersion: Version) extends SparkSQLExecute with Logging {
  private lazy val df: DataFrame = {
    // spark context will be constructed by now as this will be invoked when drda queries
    // will reach the lead node
    // TODO: KN Later get the SnappyContext as per the ctx passed to this executor
    val ctx = SnappyContext(null)

    ctx.sql(sql)
  }

  private val resultsRdd = df.queryExecution.executedPlan.execute()
  private val rowBuffer = new FlushBuffer[InternalRow](sql, senderVersion, df.schema,
    df.queryExecution.analyzed.output, SparkSQLExecuteImpl.NUM_ROWS_IN_BATCH)

  override def serializeRows(out: DataOutput) = {
    rowBuffer.serializeRows(out)
  }

  override def packRows(msg: LeadNodeExecutorMsg[_],
      snappyResultHolder: SnappyResultHolder): Unit = {
    //adds result rows to to FlushBuffer and flushes periodically
    SnappyContext.runJob(resultsRdd, (iter: Iterator[InternalRow]) => iter.toArray,
      (i, arr: Array[InternalRow]) => {
        if (arr.length > 0) {
          rowBuffer.addAndFlush(new MutableList().++(arr), msg, snappyResultHolder)
        } else {
          rowBuffer
        }
      })
    //flush remaining
    rowBuffer.flushAll(msg, snappyResultHolder)
  }
}

object SparkSQLExecuteImpl {
  var NUM_ROWS_IN_BATCH = 10000
  def unpackRows(in: DataInput) = {

  }
}

/** A class that holds data in a buffer and sends to GfxdHeapDataOutputStream (and to XD node
  * via LeadNodeExecutorMsg) when buffer reaches desired size (numRowsInBatch). The buffer will
  * be flushed chunks of size GemFireXDUtils.DML_MAX_CHUNK_SIZE, so the actual no of rows flushed
  * in one flush op could be less than numRowsInBatch
  *
  * @author shirishd
  *
  * @param sql
  * @param senderVersion
  * @param schema
  * @param numRowsInBatch
  * @tparam T
  */
class FlushBuffer[T](val sql: String, senderVersion: Version,
    schema: StructType, output : Seq[Attribute], numRowsInBatch: Int)
    extends MutableList[T] with Logging {

  logTrace(s"FlushBuffer GemFireXDUtils.DML_MAX_CHUNK_SIZE = ${GemFireXDUtils.DML_MAX_CHUNK_SIZE} " +
      s" numRowsInBatch = $numRowsInBatch")

  private lazy val hdos = new GfxdHeapDataOutputStream(Misc.getMemStore.thresholdListener(),
    sql, true, senderVersion)
  // using the gemfirexd way of sending results where in the number of
  // columns in each row is divided into sets of 8 columns. Per eight column group a
  // byte will be sent to indicate which all column in that group has a
  // non-null value.
  private lazy val (numCols, numEightColGroups, numPartCols) = getNumColumnGroups(
    this.head.asInstanceOf[InternalRow])

  /**
   * add an element to the buffer if the buffer contains desired no of rows flush those
   *
   * @param a
   * @param msg
   * @param snappyResultHolder
   * @return
   */
  def addAndFlush(a: MutableList[T], msg: LeadNodeExecutorMsg[_],
      snappyResultHolder: SnappyResultHolder): FlushBuffer[T] = {
    super.++=(a)

    if (this.size == numRowsInBatch) {
      flush(msg, snappyResultHolder)
    }
    this
  }

  /**
   * Flush all rows in the buffer
   * @param msg
   * @param snappyResultHolder
   */
  def flushAll(msg: LeadNodeExecutorMsg[_], snappyResultHolder: SnappyResultHolder): Unit = {
    //flush till buffer not empty
    while (flush(msg, snappyResultHolder) == true) {}
    //TODO: could we avoid sending metadata for dummy last result
    hdos.clearForReuse();
    sendMetaData();
    msg.lastResult(snappyResultHolder)
  }

  /**
   * Flush rows(to hdos and send further to XD node) in GemFireXDUtils.DML_MAX_CHUNK_SIZE chunks
   * and return true if the buffer not empty
   * @param msg
   * @param snappyResultHolder
   * @return
   */
  def flush(msg: LeadNodeExecutorMsg[_], snappyResultHolder: SnappyResultHolder): Boolean = {
    var dataRemaining = false
    hdos.clearForReuse()
    if (this.size == 0) {
     return dataRemaining
    }

    var numRowsSent = 0
    this.takeWhile(_ => hdos.size <= GemFireXDUtils.DML_MAX_CHUNK_SIZE).foreach(i => {
      if (numRowsSent == 0) sendMetaData()
      logTrace(s"sending row = " + i.asInstanceOf[InternalRow])
      writeRow(i.asInstanceOf[InternalRow])
      numRowsSent += 1
    })

    msg.sendResult(snappyResultHolder)
    logTrace(s"sending results no of rows sent = $numRowsSent" + numRowsSent)

    // remove the rows sent from buffer
    if (numRowsSent == this.size) {
      this.clear()
    } else {
      val (l, r) = this.splitAt(numRowsSent)
      this.clear()
      this.++=(r)
      dataRemaining = true
    }
    dataRemaining
  }

  private lazy val (tableNames, nullability) = getTableNamesAndNullability()

  def getTableNamesAndNullability():(Array[String], Array[Boolean])= {
    var i = 0
    val tables = new Array[String](output.length)
    val nullables = new Array[Boolean](output.length)
    output.foreach(a => {
      val fn = a.qualifiedName
      val dotIdx = fn.indexOf('.')
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

  private def getNumColumnGroups(row: InternalRow): (Int, Int, Int) = {
    if (row != null) {
      val nc = row.numFields
      val numGroups = (nc + 7) / 8
      val partCols = ((nc - 1) % 8) + 1
      (nc, numGroups, partCols)
    } else {
      (-1, -1, -1)
    }
  }

  private def writeRow(r: InternalRow) = {
    var groupNum: Int = 0
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
        activeByteForGroup = ActiveColumnBits.setFlagForNormalizedColumnPosition(index, activeByteForGroup)
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
      case t: DecimalType => DataSerializer.writeObject(row.getDecimal(colIndex, t.precision, t.scale), hdos)
      case FloatType => hdos.writeFloat(row.getFloat(colIndex))
      case DoubleType => hdos.writeDouble(row.getDouble(colIndex))
      case StringType => DataSerializer.writeString(row.getString(colIndex), hdos)
      // TODO: KN add varchar when that data type is identified
      // case VarCharType => StoredFormatIds.SQL_VARCHAR_ID
    }
  }

  def serializeRows(out: DataOutput) = {
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
 }

