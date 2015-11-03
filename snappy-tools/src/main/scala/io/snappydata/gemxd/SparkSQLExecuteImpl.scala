package io.snappydata.gemxd

import java.io.{IOException, DataInput, DataOutput}

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.internal.{InternalDataSerializer, HeapDataOutputStream}
import com.gemstone.gemfire.internal.shared.Version
import com.pivotal.gemfirexd.internal.engine.{GfxdConstants, Misc}
import com.pivotal.gemfirexd.internal.engine.distributed.{ActiveColumnBits, GfxdHeapDataOutputStream}
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager
import com.pivotal.gemfirexd.internal.iapi.types.SQLClob
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import com.pivotal.gemfirexd.internal.snappy.{LeadNodeExecutionContext, SparkSQLExecute}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SnappyContext, DataFrame}

/**
 * Created by kneeraj on 20/10/15.
 */
class SparkSQLExecuteImpl(val sql: String, val ctx: LeadNodeExecutionContext, senderVersion: Version) extends SparkSQLExecute {
  
  private lazy val df: DataFrame = {
    // spark context will be constructed by now as this will be invoked when drda queries
    // will reach the lead node
    // TODO: KN Later get the SnappyContext as per the ctx passed to this executor
    val ctx = SnappyContext(null, null)
    ctx.sql(sql)
  }

  private lazy val hdos = new GfxdHeapDataOutputStream(Misc.getMemStore.thresholdListener(), sql, true, senderVersion)

  private lazy val rows = df.collect()

  private var rowsSent = 0

  private lazy val totalRows = rows.size

  private val logger = Misc.getCacheLogWriterNoThrow

  // using the gemfirexd way of sending results where in the number of
  // columns in each row is divided into sets of 8 columns. Per eight column group a
  // byte will be sent to indicate which all column in that group has a
  // non-null value.
  private lazy val (numCols, numEightColGroups, numPartCols) = getNumColumnGroups(rows)

  private def getNumColumnGroups(rows: Array[Row]): (Int, Int, Int) = {
    if (rows != null && rows(0) != null) {
      val nc = rows(0).size
      val numGroups = nc / 8 + (if (nc % 8 == 0) 0 else 1)
      var partCols = nc % 8
      if (partCols == 0) {
        partCols = 8
      }
      (nc, numGroups, partCols)
    }
    else {
      (-1, -1, -1)
    }
  }

  override def getColumnNames: Array[String] = {
    df.schema.fieldNames
  }

  override def getNumColumns: Int = df.schema.size
  
  override def getColumnTypes: Array[Int] = {
    val numCols = getNumColumns
    val schema = df.schema
    val types = (0 until numCols).map( i => getSQLType(i, schema))
    types.toArray
  }

  override  def serializeRows(out: DataOutput) = {
    var numBytes = 0
    if (hdos != null) {
      numBytes = hdos.size
      InternalDataSerializer.writeArrayLength(numBytes, out)
      if (logger != null) {
        logger.info("KN: calling sendTo ", new Exception("KN:"))
      }
      hdos.sendTo(out)
    }
    else {
      InternalDataSerializer.writeArrayLength(numBytes, out)
    }
  }

  override def packRows(): Boolean = {
    if (logger != null) {
      logger.info("KN: packRows totalRows = " + totalRows + " and rowsSent = " + rowsSent)
    }
    if (totalRows == rowsSent) {
      false
    }
    else {
      if (rowsSent == 0) {
        // byte 1 will indicate that the metainfo is being packed too
        if (logger != null) {
          logger.info("KN: packRows wrote header byte 1")
        }
        hdos.writeByte(0x01);
        DataSerializer.writeStringArray(getColumnNames, hdos)
        DataSerializer.writeIntArray(getColumnTypes, hdos)
      }
      else {
        hdos.clearForReuse()
        // byte 0 will indicate that the metainfo is not being sent
        hdos.writeByte(0x00);
      }

      val start = rowsSent
      if (logger != null) {
        logger.info("KN: packRows start = " + start + " totalrows = " + totalRows + " hdos.size = " + hdos.size + " and GemFireXDUtils.DML_MAX_CHUNK_SIZE = " + GemFireXDUtils.DML_MAX_CHUNK_SIZE)
      }
      // TODO: Take care of this chunking and streaming a bit later. After verifying the functionality.
      (start until totalRows).takeWhile( _ => hdos.size <= GemFireXDUtils.DML_MAX_CHUNK_SIZE).foreach(i => {
        val r = rows(i)
        if (logger != null) {
          logger.info("KN: packRows writing row = " + rows(i))
        }
        writeRow(r, hdos)
        rowsSent += 1
      })
      true
    }
  }

  private def writeRow(r: Row, hdos: HeapDataOutputStream) = {
    var groupNum: Int = 0
    while (groupNum < numEightColGroups - 1) {
      if (logger != null) {
        logger.info("KN: writeRow group 8 " + numEightColGroups)
      }
        writeAGroup(groupNum, 8, r, hdos)
        groupNum += 1;
    }
    if (logger != null) {
      logger.info("KN: packRows write row part = " + numPartCols)
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

  private def getSQLType(i: Int, schema: StructType): Int = {
    val sf = schema(i)
    sf.dataType match {
      case TimestampType => StoredFormatIds.SQL_TIMESTAMP_ID
      case BooleanType => StoredFormatIds.SQL_BOOLEAN_ID
      case DateType => StoredFormatIds.SQL_DATE_ID
      case LongType => StoredFormatIds.SQL_LONGINT_ID
      case ShortType => StoredFormatIds.SQL_SMALLINT_ID
      case ByteType => StoredFormatIds.SQL_TINYINT_ID
      case IntegerType => StoredFormatIds.SQL_INTEGER_ID
      case t: DecimalType => StoredFormatIds.SQL_DECIMAL_ID
      case FloatType => StoredFormatIds.SQL_REAL_ID
      case DoubleType => StoredFormatIds.SQL_DOUBLE_ID
      case StringType => StoredFormatIds.SQL_CLOB_ID
      // TODO: KN add varchar when that data type is identified
      //case VarCharType => StoredFormatIds.SQL_VARCHAR_ID
    }
  }
}

object SparkSQLExecuteImpl {
  def unpackRows(in: DataInput) = {

  }
}
