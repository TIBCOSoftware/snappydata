package org.apache.spark.sql.rowtable

import java.sql.{Statement, Connection, ResultSet}
import java.util.Properties

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.collection.MultiExecutorLocalPartition
import org.apache.spark.sql.columnar.{ConnectionProperties, CachedBatch}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreFunctions._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow

/**
 * A scanner RDD which is very specific to Snappy store row tables. This scans row tables in parallel unlike Spark's
 * inbuilt JDBCRDD.
 * Most of the code is copy of JDBCRDD. We had to copy a lot of stuffs as JDBCRDD has a lot of methods as private.
 */
class RowFormatScanRDD(@transient sc: SparkContext,
    getConnection: () => Connection,
    schema: StructType,
    tableName: String,
    columns: Array[String],
    connectionProperties: ConnectionProperties,
    filters: Array[Filter] =  Array.empty[Filter],
    partitions: Array[Partition] =  Array.empty[Partition],
    blockMap: Map[InternalDistributedMember, BlockManagerId] =  Map.empty[InternalDistributedMember, BlockManagerId],
    properties: Properties = new Properties())
    extends JDBCRDD(sc, getConnection, schema, tableName, columns, filters, partitions, properties) {

  /**
   * `filters`, but as a WHERE clause suitable for injection into a SQL query.
   */
  private val filterWhereClause: String = {
    val filterStrings = filters map compileFilter filter (_ != null)
    if (filterStrings.length > 0) {
      val sb = new StringBuilder("WHERE ")
      filterStrings.foreach(x => sb.append(x).append(" AND "))
      sb.substring(0, sb.length - 5)
    } else ""
  }

  private def compileFilter(f: Filter): String = f match {
    case EqualTo(attr, value) => s"$attr = ${compileValue(value)}"
    case LessThan(attr, value) => s"$attr < ${compileValue(value)}"
    case GreaterThan(attr, value) => s"$attr > ${compileValue(value)}"
    case LessThanOrEqual(attr, value) => s"$attr <= ${compileValue(value)}"
    case GreaterThanOrEqual(attr, value) => s"$attr >= ${compileValue(value)}"
    case _ => null
  }

  /**
   * Converts value to SQL expression.
   */
  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case _ => value
  }

  private def escapeSql(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "''")


  /**
   * `columns`, but as a String suitable for injection into a SQL query.
   */
  private val columnList: String = {
    val sb = new StringBuilder()
    columns.foreach(x => sb.append(",").append(x))
    if (sb.isEmpty) "1" else sb.substring(1)
  }


  def computeResultSet(thePart: Partition): (Connection, Statement, ResultSet) = {
    val part = thePart.asInstanceOf[MultiExecutorLocalPartition]
    val conn = getConnection()

    val resolvedName = StoreUtils.lookupName(tableName, conn.getSchema)
    val region = Misc.getRegionForTable(resolvedName, true)

    if (region.isInstanceOf[PartitionedRegion]) {
      val par = part.index
      val ps1 = conn.prepareStatement(s"call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION('$resolvedName', $par)")
      val rs1 = ps1.execute()
    }


    // H2's JDBC driver does not support the setSchema() method.  We pass a
    // fully-qualified table name in the SELECT statement.  I don't know how to
    // talk about a table in a completely portable way.

    val myWhereClause = filterWhereClause

    val sqlText = s"SELECT $columnList FROM $tableName $myWhereClause"
    val stmt = conn.prepareStatement(sqlText,
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    val fetchSize = properties.getProperty("fetchSize", "0").toInt
    stmt.setFetchSize(fetchSize)
    val rs = stmt.executeQuery()
    (conn, stmt, rs)
  }


  /**
   * Runs the SQL query against the JDBC driver.
   */
  override def compute(thePart: Partition, context: TaskContext): Iterator[InternalRow] = {
    val (conn, stmt, rs) = computeResultSet(thePart)
    new InternalRowIteratorOnRS(conn,stmt, rs, context, schema).asInstanceOf[Iterator[InternalRow]]
  }


  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[MultiExecutorLocalPartition].hostExecutorIds
  }

  override def getPartitions: Array[Partition] = {
    executeWithConnection(getConnection, {
      case conn =>
        val tableSchema = conn.getSchema
        val resolvedName = StoreUtils.lookupName(tableName, tableSchema)
        val region = Misc.getRegionForTable(resolvedName, true)
        if (region.isInstanceOf[PartitionedRegion]) {
          StoreUtils.getPartitionsPartitionedTable(sc, tableName, tableSchema, blockMap)
        } else {
          StoreUtils.getPartitionsReplicatedTable(sc, resolvedName, tableSchema, blockMap)
        }
    })
  }
}



class InternalRowIteratorOnRS (conn: Connection,
    stmt: Statement, rs: ResultSet , context: TaskContext , schema:StructType)
    extends Iterator[ org.apache.spark.sql.catalyst.InternalRow]
   with  Logging {
  private val closed = false
  private var finished = false
  private var gotNext = false
  private var nextValue: InternalRow = null.asInstanceOf[InternalRow]

  context.addTaskCompletionListener { context => close() }

  val conversions = getConversions(schema)
  val mutableRow = new SpecificMutableRow(schema.fields.map(x => x.dataType))

  def getNext: InternalRow = {
    if (rs.next()) {
      var i = 0
      while (i < conversions.length) {
        val pos = i + 1
        conversions(i) match {
          case BooleanConversion => mutableRow.setBoolean(i, rs.getBoolean(pos))
          case DateConversion =>
            // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
            val dateVal = rs.getDate(pos)
            if (dateVal != null) {
              mutableRow.setInt(i, DateTimeUtils.fromJavaDate(dateVal))
            } else {
              mutableRow.update(i, null)
            }
          // When connecting with Oracle DB through JDBC, the precision and scale of BigDecimal
          // object returned by ResultSet.getBigDecimal is not correctly matched to the table
          // schema reported by ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
          // If inserting values like 19999 into a column with NUMBER(12, 2) type, you get through
          // a BigDecimal object with scale as 0. But the dataframe schema has correct type as
          // DecimalType(12, 2). Thus, after saving the dataframe into parquet file and then
          // retrieve it, you will get wrong result 199.99.
          // So it is needed to set precision and scale for Decimal based on JDBC metadata.
          case DecimalConversion(p, s) =>
            val decimalVal = rs.getBigDecimal(pos)
            if (decimalVal == null) {
              mutableRow.update(i, null)
            } else {
              mutableRow.update(i, Decimal(decimalVal, p, s))
            }
          case DoubleConversion => mutableRow.setDouble(i, rs.getDouble(pos))
          case FloatConversion => mutableRow.setFloat(i, rs.getFloat(pos))
          case IntegerConversion => mutableRow.setInt(i, rs.getInt(pos))
          case LongConversion => mutableRow.setLong(i, rs.getLong(pos))
          // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
          case StringConversion => mutableRow.update(i, UTF8String.fromString(rs.getString(pos)))
          case TimestampConversion =>
            val t = rs.getTimestamp(pos)
            if (t != null) {
              mutableRow.setLong(i, DateTimeUtils.fromJavaTimestamp(t))
            } else {
              mutableRow.update(i, null)
            }
          case BinaryConversion => mutableRow.update(i, rs.getBytes(pos))
          case BinaryLongConversion =>
            val bytes = rs.getBytes(pos)
            var ans = 0L
            var j = 0
            while (j < bytes.size) {
              ans = 256 * ans + (255 & bytes(j))
              j = j + 1
            }
            mutableRow.setLong(i, ans)
        }
        if (rs.wasNull) mutableRow.setNullAt(i)
        i = i + 1
      }
      mutableRow.asInstanceOf[InternalRow]
    } else {
      finished = true
      null.asInstanceOf[InternalRow]
    }
  }

  def close() {
    if (closed) return
    try {
      if (null != rs) {
        rs.close()
      }
    } catch {
      case e: Exception => logWarning("Exception closing resultset", e)
    }
    try {
      if (null != stmt) {
        stmt.close()
      }
    } catch {
      case e: Exception => logWarning("Exception closing statement", e)
    }
    try {
      if (null != conn) {
        conn.close()
      }
      logDebug("closed connection for task " + context.partitionId())
    } catch {
      case e: Exception => logWarning("Exception closing connection", e)
    }
  }

  override def hasNext: Boolean = {
    if (!finished) {
      if (!gotNext) {
        nextValue = getNext
        if (finished) {
          close()
        }
        gotNext = true
      }
    }
    !finished
  }

  override def next(): InternalRow = {
    if (!gotNext) {
      throw new NoSuchElementException("End of stream")
    }
    gotNext = false
    nextValue
  }

  // Each JDBC-to-Catalyst conversion corresponds to a tag defined here so that
  // we don't have to potentially poke around in the Metadata once for every
  // row.
  // Is there a better way to do this?  I'd rather be using a type that
  // contains only the tags I define.
  abstract class JDBCConversion
  case object BooleanConversion extends JDBCConversion
  case object DateConversion extends JDBCConversion
  case class  DecimalConversion(precision: Int, scale: Int) extends JDBCConversion
  case object DoubleConversion extends JDBCConversion
  case object FloatConversion extends JDBCConversion
  case object IntegerConversion extends JDBCConversion
  case object LongConversion extends JDBCConversion
  case object BinaryLongConversion extends JDBCConversion
  case object StringConversion extends JDBCConversion
  case object TimestampConversion extends JDBCConversion
  case object BinaryConversion extends JDBCConversion

  /**
   * Maps a StructType to a type tag list.
   */
  def getConversions(schema: StructType): Array[JDBCConversion] = {
    schema.fields.map(sf => sf.dataType match {
      case BooleanType => BooleanConversion
      case DateType => DateConversion
      case DecimalType.Fixed(p, s) => DecimalConversion(p, s)
      case DoubleType => DoubleConversion
      case FloatType => FloatConversion
      case IntegerType => IntegerConversion
      case LongType =>
        if (sf.metadata.contains("binarylong")) BinaryLongConversion else LongConversion
      case StringType => StringConversion
      case TimestampType => TimestampConversion
      case BinaryType => BinaryConversion
      case _ => throw new IllegalArgumentException(s"Unsupported field $sf")
    }).toArray
  }
}


