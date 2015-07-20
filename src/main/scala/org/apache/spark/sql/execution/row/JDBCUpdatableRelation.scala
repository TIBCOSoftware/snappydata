package org.apache.spark.sql.execution.row

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.{ConnectionPool, PoolProperty}
import org.apache.spark.sql.jdbc._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}
import org.apache.spark.{Logging, Partition}

/**
 * A LogicalPlan implementation for an external row table whose contents
 * are retrieved using a JDBC URL or DataSource.
 */
class JDBCUpdatableRelation(
    url: String,
    table: String,
    override val schema: StructType,
    parts: Array[Partition],
    properties: Properties,
    ddlExtensions: Option[String],
    @transient val sqlContext: SQLContext)
    extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation
    with UpdatableRelation
    with Logging {

  override val needConversion: Boolean = false

  val driver = DriverRegistry.getDriverClassName(url)

  final val dialect = JdbcDialects.get(url)

  final val schemaFields = Map(schema.fields.map { f =>
    (Utils.normalizeOptionKey(f.name), f)
  }: _*)

  final val rowInsertStr = JDBCUpdatableRelation.getInsertString(table, schema)

  // initialize GemFireXDDialect to that it gets registered
  GemFireXDDialect.init()

  // create table in external store once upfront
  createTable(url, table, properties, SaveMode.ErrorIfExists)

  def createTable(url: String, table: String,
      connectionProperties: Properties, mode: SaveMode): Unit = {
    val conn = JdbcUtils.createConnection(url, connectionProperties)
    try {
      var tableExists = JdbcUtils.tableExists(conn, table)

      if (mode == SaveMode.Ignore && tableExists) {
        return
      }

      if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }

      if (mode == SaveMode.Overwrite && tableExists) {
        // truncate the table if possible
        val truncate = dialect match {
          case MySQLDialect | PostgresDialect => s"TRUNCATE TABLE $table"
          case d: JdbcExtendedDialect => d.truncateTable(table)
          case _ => ""
        }
        if (truncate != null && truncate.length > 0) {
          conn.prepareStatement(truncate).executeUpdate()
        } else {
          JdbcUtils.dropTable(conn, table)
          tableExists = false
        }
      }

      // Create the table if the table didn't exist.
      if (!tableExists) {
        val extensions = ddlExtensions.map(" " + _).getOrElse("")
        val sql = s"CREATE TABLE $table ($schema) $extensions"
        conn.prepareStatement(sql).executeUpdate()
      }
    } finally {
      conn.close()
    }
  }

  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    new JDBCRDD(
      sqlContext.sparkContext,
      JDBCUpdatableRelation.getConnector(table, url, driver, properties),
      JDBCUpdatableRelation.pruneSchema(schema, requiredColumns),
      table,
      requiredColumns,
      filters,
      parts,
      properties)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    jdbc(data, url, table, properties,
      if (overwrite) SaveMode.Overwrite else SaveMode.Append)
  }

  def jdbc(df: DataFrame, url: String, table: String,
      connectionProperties: Properties, mode: SaveMode): Unit = {
    createTable(url, table, connectionProperties, mode)
    JDBCWriteDetails.saveTable(df, url, table, connectionProperties)
  }

  // TODO: SW: should below all be executed from driver or some random executor?

  override def insert(row: Row): Int = {
    val connection = JDBCUpdatableRelation.createConnection(table, url,
      driver, properties)
    try {
      val stmt = connection.prepareStatement(rowInsertStr)
      JDBCUpdatableRelation.setStatementParameters(stmt, schema.fields,
        row, dialect)
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }

  override def update(updatedColumns: Row, setColumns: Seq[String],
      filterExpr: String): Int = {
    val ncols = setColumns.length
    if (ncols == 0) {
      throw new IllegalArgumentException(
        "JDBCUpdatableRelation.update: no columns provided")
    }
    val setFields = new Array[StructField](ncols)
    var index = 0
    setColumns.foreach { col =>
      setFields(index) = schemaFields.getOrElse(Utils.normalizeOptionKey(col),
        throw new AnalysisException("JDBCUpdatableRelation: Cannot resolve " +
            s"column name '$col' among (${schema.fieldNames.mkString(", ")})"))
      index += 1
    }
    val connection = JDBCUpdatableRelation.createConnection(table, url,
      driver, properties)
    try {
      val setStr = setColumns.mkString("SET ", "=?, ", "=?")
      val whereStr =
        if (filterExpr == null || filterExpr.isEmpty) ""
        else "WHERE " + filterExpr
      val stmt = connection.prepareStatement(s"UPDATE $table $setStr $whereStr")
      JDBCUpdatableRelation.setStatementParameters(stmt, setFields,
        updatedColumns, dialect)
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }

  override def delete(filterExpr: String): Int = {
    val connection = JDBCUpdatableRelation.createConnection(table, url,
      driver, properties)
    try {
      val whereStr =
        if (filterExpr == null || filterExpr.isEmpty) ""
        else "WHERE " + filterExpr
      val stmt = connection.prepareStatement(s"DELETE FROM $table $whereStr")
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }

  override def destroy(): Unit = {
    // clean up the connection pool on executors first
    Utils.mapExecutors(sqlContext, { () =>
      ConnectionPool.removePoolReference(table)
      Iterator.empty
    })
    // drop the external table using a non-pool connection
    val conn = JdbcUtils.createConnection(url, properties)
    try {
      conn.createStatement().executeUpdate(s"DROP TABLE $table")
    } finally {
      conn.close()
    }
  }
}

object JDBCUpdatableRelation extends Logging {

  def getConnector(id: String, driver: String, url: String,
      properties: Properties): () => Connection = {
    () => {
      try {
        if (driver != null) DriverRegistry.register(driver)
      } catch {
        case cnfe: ClassNotFoundException =>
          logWarning(s"Couldn't find driver class $driver", cnfe)
      }
      createConnection(id, url, driver, properties)
    }
  }

  /**
   * Prune all but the specified columns from the specified Catalyst schema.
   *
   * @param schema - The Catalyst schema of the master table
   * @param columns - The list of desired columns
   *
   * @return A Catalyst schema corresponding to columns in the given order.
   */
  def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields map { x =>
      x.metadata.getString("name") -> x
    }: _*)
    new StructType(columns map { name => fieldMap(name) })
  }

  def createConnection(id: String, url: String, driver: String,
      properties: Properties): Connection = {
    ConnectionPool.getPoolDataSource(id, Map(
      PoolProperty.URL -> url,
      PoolProperty.DriverClass -> driver), properties,
      hikariCP = false).getConnection
  }

  def getInsertString(table: String, schema: StructType) = {
    val sb = new mutable.StringBuilder("INSERT INTO ")
    sb.append(table).append(" VALUES (")
    (0 until (schema.length - 1)).foreach(sb.append("?,"))
    sb.append("?)").toString()
  }

  def setStatementParameters(stmt: PreparedStatement,
      columns: Array[StructField], row: Row, dialect: JdbcDialect): Unit = {
    var col = 0
    val len = columns.length
    while (col < len) {
      val dataType = columns(col).dataType
      if (!row.isNullAt(col)) {
        dataType match {
          case IntegerType => stmt.setInt(col + 1, row.getInt(col))
          case LongType => stmt.setLong(col + 1, row.getLong(col))
          case DoubleType => stmt.setDouble(col + 1, row.getDouble(col))
          case FloatType => stmt.setFloat(col + 1, row.getFloat(col))
          case ShortType => stmt.setInt(col + 1, row.getShort(col))
          case ByteType => stmt.setInt(col + 1, row.getByte(col))
          case BooleanType => stmt.setBoolean(col + 1, row.getBoolean(col))
          case StringType => stmt.setString(col + 1, row.getString(col))
          case BinaryType =>
            stmt.setBytes(col + 1, row(col).asInstanceOf[Array[Byte]])
          case TimestampType => row(col) match {
            case ts: java.sql.Timestamp => stmt.setTimestamp(col + 1, ts)
            case s: String => stmt.setString(col + 1, s)
            case o => stmt.setObject(col + 1, o)
          }
          case DateType => row(col) match {
            case d: java.sql.Date => stmt.setDate(col + 1, d)
            case s: String => stmt.setString(col + 1, s)
            case o => stmt.setObject(col + 1, o)
          }
          case DecimalType.Fixed(_, _) | DecimalType.Unlimited => row(col) match {
            case d: Decimal => stmt.setBigDecimal(col + 1, d.toJavaBigDecimal)
            case bd: java.math.BigDecimal => stmt.setBigDecimal(col + 1, bd)
            case s: String => stmt.setString(col + 1, s)
            case o => stmt.setObject(col + 1, o)
          }
          case _ => stmt.setObject(col + 1, row(col))
        }
      } else {
        stmt.setNull(col + 1, getJDBCType(dialect, dataType))
      }
      col += 1
    }
  }

  def getJDBCType(dialect: JdbcDialect, dataType: DataType) = {
    dialect.getJDBCType(dataType).map(_.jdbcNullType).getOrElse(
      dataType match {
        case IntegerType => java.sql.Types.INTEGER
        case LongType => java.sql.Types.BIGINT
        case DoubleType => java.sql.Types.DOUBLE
        case FloatType => java.sql.Types.REAL
        case ShortType => java.sql.Types.INTEGER
        case ByteType => java.sql.Types.INTEGER
        // need to keep below mapping to BIT instead of BOOLEAN for MySQL
        case BooleanType => java.sql.Types.BIT
        case StringType => java.sql.Types.CLOB
        case BinaryType => java.sql.Types.BLOB
        case TimestampType => java.sql.Types.TIMESTAMP
        case DateType => java.sql.Types.DATE
        case DecimalType.Fixed(_, _) | DecimalType.Unlimited =>
          java.sql.Types.DECIMAL
        case NullType => java.sql.Types.NULL
        case _ => throw new IllegalArgumentException(
          s"Can't translate to JDBC value for type $dataType")
      })
  }
}

final class JDBCUpdatableSource extends SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType) = {

    val parameters = new mutable.HashMap[String, String]
    parameters ++= options
    val url = parameters.remove("url").getOrElse(
      sys.error("Option 'url' not specified"))
    // TODO: this should be optional with new DDL where tableName itself
    // will be passed and used if dbtable has not been provided
    val table = parameters.remove("dbtable").getOrElse(
      sys.error("Option 'dbtable' not specified"))
    val driver = parameters.remove("driver")
    val ddlExtensions = parameters.remove("ddlextensions")
    val partitionColumn = parameters.remove("partitionColumn")
    val lowerBound = parameters.remove("lowerBound")
    val upperBound = parameters.remove("upperBound")
    val numPartitions = parameters.remove("numPartitions")

    driver.foreach(DriverRegistry.register)

    val partitionInfo = if (partitionColumn.isEmpty) {
      null
    } else {
      if (lowerBound.isEmpty || upperBound.isEmpty || numPartitions.isEmpty) {
        sys.error("Partitioning incompletely specified")
      }
      JDBCPartitioningInfo(
        partitionColumn.get,
        lowerBound.get.toLong,
        upperBound.get.toLong,
        numPartitions.get.toInt)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo)
    // remaining parameters are passed as properties to getConnection
    val properties = new Properties()
    parameters.foreach(kv => properties.setProperty(kv._1, kv._2))
    new JDBCUpdatableRelation(url, table, schema, parts, properties,
      ddlExtensions, sqlContext)
  }
}
