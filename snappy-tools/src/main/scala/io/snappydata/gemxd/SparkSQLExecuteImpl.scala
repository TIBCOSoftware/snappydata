package io.snappydata.gemxd

import java.io.{DataInput, DataOutput}

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.internal.shared.Version
import com.gemstone.gemfire.internal.{HeapDataOutputStream, InternalDataSerializer}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.distributed.{ActiveColumnBits, GfxdHeapDataOutputStream}
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import com.pivotal.gemfirexd.internal.snappy.{LeadNodeExecutionContext, SparkSQLExecute}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SnappyContext}

/**
 * Created by kneeraj on 20/10/15.
 */
class SparkSQLExecuteImpl(val sql: String, val ctx: LeadNodeExecutionContext, senderVersion: Version) extends SparkSQLExecute {

  private lazy val df: DataFrame = {
    // spark context will be constructed by now as this will be invoked when drda queries
    // will reach the lead node
    // TODO: KN Later get the SnappyContext as per the ctx passed to this executor
    val ctx = SnappyContext()
    ctx.sql(sql)
  }

  private lazy val hdos = new GfxdHeapDataOutputStream(Misc.getMemStore.thresholdListener(), sql, true, senderVersion)

  private lazy val rows = df.collect()

  private var rowsSent = 0

  private lazy val totalRows = rows.size

  // using the gemfirexd way of sending results where in the number of
  // columns in each row is divided into sets of 8 columns. Per eight column group a
  // byte will be sent to indicate which all column in that group has a
  // non-null value.
  private lazy val (numCols, numEightColGroups, numPartCols) = getNumColumnGroups(rows)

  private def getNumColumnGroups(rows: Array[Row]): (Int, Int, Int) = {
    if (rows != null && rows(0) != null) {
      val nc = rows(0).size
      val numGroups = (nc + 7) / 8
      val partCols = ((nc - 1) % 8) + 1
      (nc, numGroups, partCols)
    }
    else {
      (-1, -1, -1)
    }
  }

  private def getColumnNames: Array[String] = {
    df.schema.fieldNames
  }

  private def getNumColumns: Int = df.schema.size

  private def getColumnTypes: Array[(Int, Int, Int)] = {
    val numCols = getNumColumns
    val schema = df.schema
    (0 until numCols).map(i => getSQLType(i, schema)).toArray
  }

  override def serializeRows(out: DataOutput) = {
    var numBytes = 0
    if (hdos != null) {
      numBytes = hdos.size
      InternalDataSerializer.writeArrayLength(numBytes, out)
      hdos.sendTo(out)
    }
    else {
      InternalDataSerializer.writeArrayLength(numBytes, out)
    }
  }

  override def packRows(): Boolean = {
    if (totalRows == rowsSent) {
      false
    }
    else {
      if (rowsSent == 0) {
        // Just send the metadata once
        val x  = df.queryExecution.analyzed.output
        val tableNames = new Array[String](x.length)
        val nullability = new Array[Boolean](x.length)
        var i = 0
        x.foreach( a => {
          val fn = a.qualifiedName
          val dotIdx = fn.indexOf('.')
          if (dotIdx > 0) {
            val tname = fn.substring(0, dotIdx)
            tableNames(i) = tname
          }
          else {
            tableNames(i) = ""
          }
          nullability(i) = a.nullable
          i = i + 1
        })
        // byte 1 will indicate that the metainfo is being packed too
        hdos.writeByte(0x01);
        DataSerializer.writeStringArray(tableNames, hdos)
        DataSerializer.writeStringArray(getColumnNames, hdos)
        DataSerializer.writeBooleanArray(nullability, hdos)
        val colTypes = getColumnTypes
        colTypes.foreach(x => {
          val t = x._1
          InternalDataSerializer.writeSignedVL(t, hdos)
          if ( t == StoredFormatIds.SQL_DECIMAL_ID) {
            InternalDataSerializer.writeSignedVL(x._2, hdos) // precision
            InternalDataSerializer.writeSignedVL(x._3, hdos) // scale
          }
        })
      }
      else {
        hdos.clearForReuse()
        // byte 0 will indicate that the metainfo is not being sent
        hdos.writeByte(0x00);
      }

      val start = rowsSent
      // TODO: Take care of this chunking and streaming a bit later. After verifying the functionality.
      (start until totalRows).takeWhile(_ => hdos.size <= GemFireXDUtils.DML_MAX_CHUNK_SIZE).foreach(i => {
        val r = rows(i)
        writeRow(r, hdos)
        rowsSent += 1
      })
      true
    }
  }

  private def writeRow(r: Row, hdos: HeapDataOutputStream) = {
    var groupNum: Int = 0
    while (groupNum < numEightColGroups - 1) {
      writeAGroup(groupNum, 8, r, hdos)
      groupNum += 1;
    }
    writeAGroup(groupNum, numPartCols, r, hdos)
  }

  private def writeAGroup(groupNum: Int, numColsInGrp: Int, row: Row, dos: DataOutput) {
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

  private def writeColDataInOptimizedWay(row: Row, colIndex: Int, out: DataOutput) = {
    val sf = df.schema(colIndex)
    sf.dataType match {
      case TimestampType => InternalDataSerializer.writeSignedVL(row.getLong(colIndex), hdos)
      case BooleanType => hdos.writeBoolean(row.getBoolean(colIndex))
      case DateType => InternalDataSerializer.writeSignedVL(row.getLong(colIndex), hdos)
      case LongType => InternalDataSerializer.writeSignedVL(row.getLong(colIndex), hdos)
      case ShortType => InternalDataSerializer.writeSignedVL(row.getInt(colIndex), hdos)
      case ByteType => DataSerializer.writePrimitiveByte(row.getByte(colIndex), hdos)
      case IntegerType => InternalDataSerializer.writeSignedVL(row.getInt(colIndex), hdos)
      case t: DecimalType => DataSerializer.writeObject(row.getDecimal(colIndex), hdos)
      case FloatType => hdos.writeFloat(row.getFloat(colIndex))
      case DoubleType => hdos.writeDouble(row.getDouble(colIndex))
      case StringType => DataSerializer.writeString(row.getString(colIndex), hdos)
      // TODO: KN add varchar when that data type is identified
      // case VarCharType => StoredFormatIds.SQL_VARCHAR_ID
    }
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
  def unpackRows(in: DataInput) = {

  }
}
