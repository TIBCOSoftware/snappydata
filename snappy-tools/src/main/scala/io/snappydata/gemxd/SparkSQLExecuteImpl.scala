package io.snappydata.gemxd

import java.io.{DataInput, DataOutput}

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.internal.HeapDataOutputStream
import com.gemstone.gemfire.internal.shared.Version
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdHeapDataOutputStream
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import com.pivotal.gemfirexd.internal.snappy.{LeadNodeExecutionContext, SparkSQLExecute}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SnappyContext, DataFrame}

/**
 * Created by kneeraj on 20/10/15.
 */
class SparkSQLExecuteImpl(val sql: String, val ctx: LeadNodeExecutionContext, val senderVersion: Version) extends SparkSQLExecute {
  
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

  private lazy val allRows = rows.size

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

  override def packRows(out: DataOutput): Unit = {
    if (rowsSent == 0) {
      DataSerializer.writeStringArray(getColumnNames, out)
      DataSerializer.writeIntArray(getColumnTypes, out)
    }
    else {
      hdos.clearForReuse()
    }

    val start = rowsSent
    (start until allRows).takeWhile( _ => hdos.size >= GemFireXDUtils.DML_MAX_CHUNK_SIZE ).foreach( i => {
      val r = rows(i)
      writeRow(r, hdos)
      rowsSent += 1
    })
  }

  private def writeRow(r: Row, hdos: HeapDataOutputStream) = {

  }

  private def getSQLType(i: Int, schema: StructType): Int = {
    val sf = schema(i)
    // TODO: This is bad. Fix later.
    if (sf.dataType.typeName.startsWith("decimal")) {
      StoredFormatIds.SQL_DECIMAL_ID
    }
    else {
      sf.dataType match {
        case TimestampType => StoredFormatIds.SQL_TIMESTAMP_ID
        case BooleanType => StoredFormatIds.SQL_BOOLEAN_ID
        case DateType => StoredFormatIds.SQL_DATE_ID
        case LongType => StoredFormatIds.SQL_LONGINT_ID
        case ShortType => StoredFormatIds.SQL_SMALLINT_ID
        case ByteType => StoredFormatIds.SQL_INTEGER_ID
        case IntegerType => StoredFormatIds.SQL_INTEGER_ID
        // This case giving compilation error as internal type is decimal
        //case DecimalType => StoredFormatIds.SQL_DECIMAL_ID
        case FloatType => StoredFormatIds.SQL_REAL_ID
        case DoubleType => StoredFormatIds.SQL_DOUBLE_ID
        case StringType => StoredFormatIds.SQL_CLOB_ID
        // TODO: KN add varchar when that data type is identified
        //case VarCharType => StoredFormatIds.SQL_VARCHAR_ID
      }
    }
  }
}

object SparkSQLExecuteImpl {
  def unpackRows(in: DataInput) = {

  }
}
