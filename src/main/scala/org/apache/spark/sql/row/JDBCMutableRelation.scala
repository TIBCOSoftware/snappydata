package org.apache.spark.sql.row

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.{Logging, Partition}

/**
 * A LogicalPlan implementation for an external row table whose contents
 * are retrieved using a JDBC URL or DataSource.
 */
class JDBCMutableRelation(
    val url: String,
    val table: String,
    val provider: String,
    mode: SaveMode,
    userSpecifiedString: String,
    parts: Array[Partition],
    _poolProps: Map[String, String],
    val connProperties: Properties,
    val hikariCP: Boolean,
    val origOptions: Map[String, String],
    @transient override val sqlContext: SQLContext)
    extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation
    with RowInsertableRelation
    with UpdatableRelation
    with DeletableRelation
    with Logging {

  override val needConversion: Boolean = false

  val driver = DriverRegistry.getDriverClassName(url)

  private[this] val poolProperties = ExternalStoreUtils
      .getAllPoolProperties(url, driver, _poolProps, hikariCP)

  final val dialect = JdbcDialects.get(url)

  // create table in external store once upfront
  createTable(mode)

  override val schema: StructType =
    JDBCRDD.resolveTable(url, table, connProperties)

  final val schemaFields = Map(schema.fields.flatMap { f =>
    val name =
      if (f.metadata.contains("name")) f.metadata.getString("name") else f.name
    val nname = Utils.normalizeId(name)
    if (name != nname) {
      Iterator((name, f), (Utils.normalizeId(name), f))
    } else {
      Iterator((name, f))
    }
  }: _*)

  def createTable(mode: SaveMode): Unit = {
    var conn: Connection = null
    try {
      conn = JdbcUtils.createConnection(url, connProperties)
      var tableExists = JdbcExtendedUtils.tableExists(conn, table,
        dialect, sqlContext)
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
          JdbcExtendedUtils.executeUpdate(truncate, conn)
        } else {
          JdbcUtils.dropTable(conn, table)
          tableExists = false
        }
      }

      // Create the table if the table didn't exist.
      if (!tableExists) {
        val sql = s"CREATE TABLE $table $userSpecifiedString"
        JdbcExtendedUtils.executeUpdate(sql, conn)
      }
    } catch {
      case sqle: java.sql.SQLException =>
        if (sqle.getMessage.contains("No suitable driver found")) {
          throw new AnalysisException(s"${sqle.getMessage}\n" +
              "Ensure that the 'driver' option is set appropriately and " +
              "the driver jars available (--jars option in spark-submit).")
        } else {
          throw sqle
        }
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }

  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    new JDBCRDD(
      sqlContext.sparkContext,
      JDBCMutableRelation.getConnector(table, driver, poolProperties,
        connProperties, hikariCP),
      JDBCMutableRelation.pruneSchema(schemaFields, requiredColumns),
      table,
      requiredColumns,
      filters,
      parts,
      connProperties).asInstanceOf[RDD[Row]]
  }

  final val rowInsertStr = JDBCMutableRelation.getInsertString(table, schema)

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    insert(data, if (overwrite) SaveMode.Overwrite else SaveMode.Append)
  }

  def insert(data: DataFrame, mode: SaveMode): Unit = {
    createTable(mode)
    insert(data)
  }

  def insert(data: DataFrame): Unit = {
    JDBCWriteDetails.saveTable(data, url, table, connProperties)
  }

  // TODO: SW: should below all be executed from driver or some random executor?
  // at least the insert can be split into batches and modelled as an RDD

  override def insert(rows: Seq[Row]): Int = {
    val numRows = rows.length
    if (numRows == 0) {
      throw new IllegalArgumentException(
        "JDBCUpdatableRelation.insert: no rows provided")
    }
    val connection = ConnectionPool.getPoolConnection(table,
      poolProperties, connProperties, hikariCP)
    try {
      val stmt = connection.prepareStatement(rowInsertStr)
      if (numRows > 1) {
        for (row <- rows) {
          JDBCMutableRelation.setStatementParameters(stmt, schema.fields,
            row, dialect)
          stmt.addBatch()
        }
      } else {
        JDBCMutableRelation.setStatementParameters(stmt, schema.fields,
          rows.head, dialect)
      }
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }

  override def executeUpdate(sql: String): Int = {
    val connection = ConnectionPool.getPoolConnection(table,
      poolProperties, connProperties, hikariCP)
    try {
      val stmt = connection.prepareStatement(sql)
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }

  override def update(filterExpr: String, newColumnValues: Row,
      updateColumns: Seq[String]): Int = {
    val ncols = updateColumns.length
    if (ncols == 0) {
      throw new IllegalArgumentException(
        "JDBCUpdatableRelation.update: no columns provided")
    }
    val setFields = new Array[StructField](ncols)
    var index = 0
    // not using loop over index below because incoming Seq[...]
    // may not have efficient index lookup
    updateColumns.foreach { col =>
      setFields(index) = schemaFields.getOrElse(col, schemaFields.getOrElse(
        Utils.normalizeId(col), throw new AnalysisException(
          "JDBCUpdatableRelation: Cannot resolve column name " +
              s""""$col" among (${schema.fieldNames.mkString(", ")})""")))
      index += 1
    }
    val connection = ConnectionPool.getPoolConnection(table,
      poolProperties, connProperties, hikariCP)
    try {
      val setStr = updateColumns.mkString("SET ", "=?, ", "=?")
      val whereStr =
        if (filterExpr == null || filterExpr.isEmpty) ""
        else " WHERE " + filterExpr
      val stmt = connection.prepareStatement(s"UPDATE $table $setStr$whereStr")
      JDBCMutableRelation.setStatementParameters(stmt, setFields,
        newColumnValues, dialect)
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }

  override def delete(filterExpr: String): Int = {
    val connection = ConnectionPool.getPoolConnection(table,
      poolProperties, connProperties, hikariCP)
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

  override def destroy(ifExists: Boolean): Unit = {
    // clean up the connection pool on executors first
    Utils.mapExecutors(sqlContext,
      JDBCMutableRelation.removePool(table)).count()
    // drop the external table using a non-pool connection
    val conn = JdbcUtils.createConnection(url, connProperties)
    try {
      JdbcExtendedUtils.dropTable(conn, table, dialect, sqlContext, ifExists)
    } finally {
      conn.close()
    }
  }
}

object JDBCMutableRelation extends Logging {

  def getConnector(id: String, driver: String, poolProps: Map[String, String],
      connProps: Properties, hikariCP: Boolean): () => Connection = {
    () => {
      try {
        if (driver != null) DriverRegistry.register(driver)
      } catch {
        case cnfe: ClassNotFoundException =>
          logWarning(s"Couldn't find driver class $driver", cnfe)
      }
      ConnectionPool.getPoolConnection(id, poolProps, connProps, hikariCP)
    }
  }

  private def removePool(table: String): () => Iterator[Unit] = () => {
    ConnectionPool.removePoolReference(table)
    Iterator.empty
  }

  /**
   * Prune all but the specified columns from the specified Catalyst schema.
   *
   * @param fieldMap - The Catalyst column name to metadata of the master table
   * @param columns - The list of desired columns
   *
   * @return A Catalyst schema corresponding to columns in the given order.
   */
  def pruneSchema(fieldMap: Map[String, StructField],
      columns: Array[String]): StructType = {
    new StructType(columns.map { col =>
      fieldMap.getOrElse(col, fieldMap.getOrElse(Utils.normalizeId(col),
        throw new AnalysisException("JDBCUpdatableRelation: Cannot resolve " +
            s"""column name "$col" among (${fieldMap.keys.mkString(", ")})""")
      ))
    })
  }

  def getInsertString(table: String, schema: StructType) = {
    val sb = new mutable.StringBuilder("INSERT INTO ")
    sb.append(table).append(" VALUES (")
    (1 until schema.length).foreach(sb.append("?,"))
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
          case DecimalType.Fixed(_, _) | DecimalType.Unlimited =>
            row(col) match {
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

final class DefaultSource
    extends ExternalSchemaRelationProvider
    with SchemaRelationProvider
    with RelationProvider
    with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: String) = {
    val parameters = new mutable.HashMap[String, String]
    parameters ++= options

    val url = parameters.remove("url")
        .getOrElse(sys.error("JDBC URL option 'url' not specified"))
    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    val table = parameters.remove(dbtableProp)
        .getOrElse(sys.error(s"Option '$dbtableProp' not specified"))
    val driver = parameters.remove("driver")
    val poolImpl = parameters.remove("poolimpl")
    val poolProperties = parameters.remove("poolproperties")
    val partitionColumn = parameters.remove("partitioncolumn")
    val lowerBound = parameters.remove("lowerbound")
    val upperBound = parameters.remove("upperbound")
    val numPartitions = parameters.remove("numpartitions")
    // remove ALLOW_EXISTING property, if remaining
    parameters.remove(JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY)

    driver.foreach(DriverRegistry.register)

    val hikariCP = poolImpl.map(Utils.normalizeId) match {
      case Some("hikari") => true
      case Some("tomcat") => false
      case Some(p) =>
        throw new IllegalArgumentException("JDBCUpdatableRelation: " +
            s"unsupported pool implementation '$p' " +
            s"(supported values: tomcat, hikari)")
      case None => false
    }
    val poolProps = poolProperties.map(p => Map(p.split(",").map { s =>
      val eqIndex = s.indexOf('=')
      if (eqIndex >= 0) {
        (s.substring(0, eqIndex).trim, s.substring(eqIndex + 1).trim)
      } else {
        // assume a boolean property to be enabled
        (s.trim, "true")
      }
    }: _*)).getOrElse(Map.empty)

    val partitionInfo = if (partitionColumn.isEmpty) {
      null
    } else {
      if (lowerBound.isEmpty || upperBound.isEmpty || numPartitions.isEmpty) {
        throw new IllegalArgumentException("JDBCUpdatableRelation: " +
            "incomplete partitioning specified")
      }
      JDBCPartitioningInfo(
        partitionColumn.get,
        lowerBound.get.toLong,
        upperBound.get.toLong,
        numPartitions.get.toInt)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo)
    // remaining parameters are passed as properties to getConnection
    val connProps = new Properties()
    parameters.foreach(kv => connProps.setProperty(kv._1, kv._2))
    new JDBCMutableRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema, parts,
      poolProps, connProps, hikariCP, options, sqlContext)
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType) = {
    val url = options.getOrElse("url", sys.error("Option 'url' not specified"))
    val dialect = JdbcDialects.get(url)
    val schemaString = JdbcExtendedUtils.schemaString(schema, dialect)

    val allowExisting = options.get(JdbcExtendedUtils
        .ALLOW_EXISTING_PROPERTY).exists(_.toBoolean)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    createRelation(sqlContext, mode, options, schemaString)
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String]) = {
    val allowExisting = options.get(JdbcExtendedUtils
        .ALLOW_EXISTING_PROPERTY).exists(_.toBoolean)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    // will work only if table is already existing
    createRelation(sqlContext, mode, options, "")
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], data: DataFrame) = {
    val url = options.getOrElse("url", sys.error("Option 'url' not specified"))
    val dialect = JdbcDialects.get(url)
    val schemaString = JdbcExtendedUtils.schemaString(data.schema, dialect)

    val relation = createRelation(sqlContext, mode, options, schemaString)
    relation.insert(data)
    relation
  }
}
