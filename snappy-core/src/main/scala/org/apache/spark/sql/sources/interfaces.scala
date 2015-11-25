package org.apache.spark.sql.sources

import java.sql.Connection
import java.util.Properties

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCRelation, JDBCPartitioningInfo, DriverRegistry}
import org.apache.spark.sql.execution.datasources.{CaseInsensitiveMap, ResolvedDataSource}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.{JdbcDialects, JdbcDialect}
import org.apache.spark.sql.row.JDBCMutableRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, AnalysisException, Row, SQLContext, SaveMode}

@DeveloperApi
trait RowInsertableRelation {

  /**
   * Insert a sequence of rows into the table represented by this relation.
   *
   * @param rows the rows to be inserted
   *
   * @return number of rows inserted
   */
  def insert(rows: Seq[Row]): Int
}

@DeveloperApi
trait UpdatableRelation {

  /**
   * Execute a DML SQL and return the number of rows affected.
   */
  def executeUpdate(sql: String): Int

  /**
   * Update a set of rows matching given criteria.
   *
   * @param filterExpr SQL WHERE criteria to select rows that will be updated
   * @param newColumnValues updated values for the columns being changed;
   *                        must match `updateColumns`
   * @param updateColumns the columns to be updated; must match `updatedColumns`
   *
   * @return number of rows affected
   */
  def update(filterExpr: String, newColumnValues: Row,
      updateColumns: Seq[String]): Int
}

@DeveloperApi
trait DeletableRelation {

  /**
   * Delete a set of row matching given criteria.
   *
   * @param filterExpr SQL WHERE criteria to select rows that will be deleted
   *
   * @return number of rows deleted
   */
  def delete(filterExpr: String): Int

}

@DeveloperApi
trait DestroyRelation {

  /**
   * Truncate the table represented by this relation.
   */
  def truncate(): Unit
  /**
   * Destroy and cleanup this relation. It may include, but not limited to,
   * dropping the external table that this relation represents.
   */
  def destroy(ifExists: Boolean): Unit
}

/**
 * ::DeveloperApi::
 * Implemented by objects that produce relations for a specific kind of data
 * source with a given schema.  When Spark SQL is given a DDL operation with
 * a USING clause specified (to specify the implemented SchemaRelationProvider)
 * and a user defined schema, this interface is used to pass in the parameters
 * specified by a user.
 *
 * Users may specify the fully qualified class name of a given data source.
 * When that class is not found Spark SQL will append the class name
 * `DefaultSource` to the path, allowing for less verbose invocation.
 * For example, 'org.apache.spark.sql.json' would resolve to the data source
 * 'org.apache.spark.sql.json.DefaultSource'.
 *
 * A new instance of this class with be instantiated each time a DDL call is made.
 *
 * The difference between a [[SchemaRelationProvider]] and an
 * [[ExternalSchemaRelationProvider]] is that latter accepts schema and other
 * clauses in DDL string and passes over to the backend as is, while the schema
 * specified for former is parsed by Spark SQL.
 * A relation provider can inherit both [[SchemaRelationProvider]] and
 * [[ExternalSchemaRelationProvider]] if it can support both Spark SQL schema
 * and backend-specific schema.
 */
@DeveloperApi
trait ExternalSchemaRelationProvider {
  /**
   * Returns a new base relation with the given parameters and user defined
   * schema (and possibly other backend-specific clauses).
   * Note: the parameters' keywords are case insensitive and this insensitivity
   * is enforced by the Map that is passed to the function.
   */
  def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      schema: String): BaseRelation
}

/**
 * Some extensions to `JdbcDialect` used by Snappy implementation.
 */
abstract class JdbcExtendedDialect extends JdbcDialect {

  /** Query string to check for existence of a table */
  def tableExists(tableName: String, conn: Connection,
      context: SQLContext): Boolean

  /** DDL to truncate a table, or null/empty if truncate is not supported */
  def truncateTable(tableName: String): String = s"TRUNCATE TABLE $tableName"

  def dropTable(tableName: String, conn: Connection, context: SQLContext,
      ifExists: Boolean): Unit

  def initializeTable(tableName: String, caseSensitive: Boolean,
      conn: Connection): Unit = {
  }

  def extraCreateTableProperties(isLoner: Boolean): Properties =
    new Properties()

  def getPartitionByClause(col : String) : String;
}

object JdbcExtendedUtils {

  val DBTABLE_PROPERTY = "dbtable"
  val SCHEMA_PROPERTY = "schemaddl"
  val ALLOW_EXISTING_PROPERTY = "allowexisting"

  def executeUpdate(sql: String, conn: Connection): Unit = {
    val stmt = conn.createStatement()
    try {
      stmt.executeUpdate(sql)
    } finally {
      stmt.close()
    }
  }

  /**
   * Compute the schema string for this RDD.
   */
  def schemaString(schema: StructType, dialect: JdbcDialect): String = {
    val sb = new StringBuilder()
    schema.fields.foreach { field =>
      val dataType = field.dataType
      val typeString: String =
        dialect.getJDBCType(dataType).map(_.databaseTypeDefinition).getOrElse(
          dataType match {
            case IntegerType => "INTEGER"
            case LongType => "BIGINT"
            case DoubleType => "DOUBLE PRECISION"
            case FloatType => "REAL"
            case ShortType => "INTEGER"
            case ByteType => "BYTE"
            case BooleanType => "BIT(1)"
            case StringType => "TEXT"
            case BinaryType => "BLOB"
            case TimestampType => "TIMESTAMP"
            case DateType => "DATE"
            case DecimalType.Fixed(precision, scale) =>
              s"DECIMAL($precision,$scale)"
            case _ => throw new IllegalArgumentException(
              s"Don't know how to save $field to JDBC")
          })
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", ${field.name} $typeString $nullable")
    }
    if (sb.length < 2) "" else "(".concat(sb.substring(2)).concat(")")
  }

  /**
   * Returns true if the table already exists in the JDBC database.
   */
  def tableExists(conn: Connection, table: String, dialect: JdbcDialect,
      context: SQLContext): Boolean = {
    dialect match {
      case d: JdbcExtendedDialect => d.tableExists(table, conn, context)

      case _ =>
        try {
          val stmt = conn.createStatement()
          val rs = stmt.executeQuery(s"SELECT 1 FROM $table LIMIT 1")
          rs.next()
          rs.close()
          stmt.close()
          true
        } catch {
          case NonFatal(e) => false
        }
    }
  }

  def dropTable(conn: Connection, tableName: String, dialect: JdbcDialect,
      context: SQLContext, ifExists: Boolean): Unit = {
    dialect match {
      case d: JdbcExtendedDialect =>
        d.dropTable(tableName, conn, context, ifExists)
      case _ =>
        if (!ifExists || tableExists(conn, tableName, dialect, context)) {
          JdbcExtendedUtils.executeUpdate(s"DROP TABLE $tableName", conn)
        }
    }
  }

  def truncateTable(conn: Connection, tableName: String, dialect: JdbcDialect): Unit = {
    dialect match {
      case d: JdbcExtendedDialect =>
        JdbcExtendedUtils.executeUpdate(d.truncateTable(tableName), conn)
      case _ =>
        JdbcExtendedUtils.executeUpdate(s"TRUNCATE TABLE $tableName", conn)
    }
  }

  /**
   * Create a [[ResolvedDataSource]] for an external DataSource schema DDL
   * string specification.
   */
  def externalResolvedDataSource(
      sqlContext: SQLContext,
      schemaString: String,
      provider: String,
      mode: SaveMode,
      options: Map[String, String]): ResolvedDataSource = {
    val clazz: Class[_] = ResolvedDataSource.lookupDataSource(provider)
    val relation = clazz.newInstance() match {

      case dataSource: ExternalSchemaRelationProvider =>
        // add schemaString as separate property for Hive persistence
        dataSource.createRelation(sqlContext, mode, new CaseInsensitiveMap(
          options + (SCHEMA_PROPERTY -> schemaString)), schemaString)

      case _ => throw new AnalysisException(
        s"${clazz.getCanonicalName} is not an ExternalSchemaRelationProvider.")
    }
    new ResolvedDataSource(clazz, relation)
  }
}

abstract class MutableRelationProvider extends ExternalSchemaRelationProvider
with SchemaRelationProvider with RelationProvider with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: String) = {
    val parameters = new mutable.HashMap[String, String]
    parameters ++= options

    val url = parameters.remove("url")
        .getOrElse(sys.error("JDBC URL option 'url' not specified"))
    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    parameters.remove("serialization.format")
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
    parameters.remove(JdbcExtendedUtils.SCHEMA_PROPERTY)

    parameters.remove("serialization.format")

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
    val (url, _, _, _, _) =
      ExternalStoreUtils.validateAndGetAllProps(sqlContext.sparkContext, options)
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
    val (url, _, _, _, _) =
      ExternalStoreUtils.validateAndGetAllProps(sqlContext.sparkContext, options)
    val dialect = JdbcDialects.get(url)
    val schemaString = JdbcExtendedUtils.schemaString(data.schema, dialect)

    val relation = createRelation(sqlContext, mode, options, schemaString)
    relation.insert(data)
    relation
  }
}