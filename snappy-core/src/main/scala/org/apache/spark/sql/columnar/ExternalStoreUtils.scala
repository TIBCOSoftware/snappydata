package org.apache.spark.sql.columnar

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import scala.StringBuilder
import scala.collection.mutable

import io.snappydata.Constant

import org.apache.spark.sql.collection.{ToolsCallbackInit, Utils}
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.row.{GemFireXDClientDialect, GemFireXDDialect}
import org.apache.spark.sql.sources.{JdbcExtendedDialect, JdbcExtendedUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Row, SnappyContext, _}
import org.apache.spark.{Logging, SparkContext}
/**
 * Utility methods used by external storage layers.
 */
private[sql] object ExternalStoreUtils extends Logging {

  def getAllPoolProperties(url: String, driver: String,
      poolProps: Map[String, String], hikariCP: Boolean) = {
    val urlProp = if (hikariCP) "jdbcUrl" else "url"
    val driverClassProp = "driverClassName"
    val props = {
      if (driver == null || driver.isEmpty) {
        if (poolProps.isEmpty) {
          Map(urlProp -> url)
        } else {
          poolProps + (urlProp -> url)
        }
      } else if (poolProps.isEmpty) {
        Map(urlProp -> url, driverClassProp -> driver)
      } else {
        poolProps + (urlProp -> url) + (driverClassProp -> driver)
      }
    }
    if (hikariCP) {
      props + ("minimumIdle" -> "4")
    } else {
      props + ("initialSize" -> "4")
    }
  }

  def getDriver(url: String, dialect: JdbcDialect): String = {
    dialect match {
      case GemFireXDDialect => "com.pivotal.gemfirexd.jdbc.EmbeddedDriver"
      case GemFireXDClientDialect => "com.pivotal.gemfirexd.jdbc.ClientDriver"
      case _ => DriverRegistry.getDriverClassName(url)
    }
  }

  class CaseInsensitiveMutableHashMap(map: Map[String, String])
      extends mutable.Map[String, String] with Serializable {

    val baseMap = new mutable.HashMap[String, String]
    baseMap ++= map.map(kv => kv.copy(_1 = kv._1.toLowerCase))

    override def get(k: String): Option[String] = baseMap.get(k.toLowerCase)

    override def remove(k: String): Option[String] = baseMap.remove(k.toLowerCase)

    override def iterator: Iterator[(String, String)] = baseMap.iterator

    override def +=(kv: (String, String)) = {
      baseMap += kv
      this
    }

    override def -=(key: String) = {
      baseMap -= key
      this
    }
  }

  def removeInternalProps(parameters: mutable.Map[String, String]): String = {
    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    val table = parameters.remove(dbtableProp)
        .getOrElse(sys.error(s"Option '$dbtableProp' not specified"))
    parameters.remove(JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY)
    parameters.remove(JdbcExtendedUtils.SCHEMA_PROPERTY)
    parameters.remove("serialization.format")
    table
  }

  def defaultStoreURL(sc: SparkContext): String = {
    SnappyContext.getClusterMode(sc) match {
      case SnappyEmbeddedMode(_, _) =>
        // Already connected to SnappyData in embedded mode.
        Constant.DEFAULT_EMBEDDED_URL + ";host-data=false;mcast-port=0"
      case SnappyShellMode(_, _) =>
        ToolsCallbackInit.toolsCallback.getLocatorJDBCURL(sc) +
            "/route-query=false"
      case ExternalEmbeddedMode(_, url) =>
        Constant.DEFAULT_EMBEDDED_URL + ";host-data=false;" + url
      case LocalMode(_, url) =>
        Constant.DEFAULT_EMBEDDED_URL + ';' + url
      case ExternalClusterMode(_, url) =>
        throw new AnalysisException("Option 'url' not specified for cluster " +
            url)
    }
  }

  def isExternalShellMode(sparkContext: SparkContext): Boolean = {
    SnappyContext.getClusterMode(sparkContext) match {
      case SnappyShellMode(_, _) => true
      case _ => false
    }
  }

  def isNotEmbeddedMode(sparkContext: SparkContext): Boolean = {
    SnappyContext.getClusterMode(sparkContext) match {
      case SnappyShellMode(_, _) | LocalMode(_, _) => true
      case _ => false
    }
  }


  def validateAndGetAllProps(sc : SparkContext,
      parameters: mutable.Map[String, String]) = {

    val url = parameters.remove("url").getOrElse(defaultStoreURL(sc))

    val dialect = JdbcDialects.get(url)
    val driver = parameters.remove("driver").getOrElse(getDriver(url, dialect))

    DriverRegistry.register(driver)

    val poolImpl = parameters.remove("poolimpl")
    val poolProperties = parameters.remove("poolproperties")

    val hikariCP = poolImpl.map(Utils.normalizeId) match {
      case Some("hikari") => true
      case Some("tomcat") => false
      case Some(p) =>
        throw new IllegalArgumentException("ExternalStoreUtils: " +
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

    // remaining parameters are passed as properties to getConnection
    val connProps = new Properties()
    parameters.foreach(kv => connProps.setProperty(kv._1, kv._2))
    dialect match {
      case GemFireXDClientDialect =>
        connProps.setProperty("route-query", "false")
      case _ =>
    }
    val allPoolProps = getAllPoolProperties(url, driver,
      poolProps, hikariCP)
    (url, driver, allPoolProps, connProps, hikariCP)
  }

   def getConnection(url: String, connProperties: Properties,
      driverDialect: JdbcDialect, isLoner: Boolean) = {

    connProperties.remove("poolProperties")
    if (driverDialect != null) {
      // add driver specific properties
      driverDialect match {
        // The driver if not a loner should be an accesor only
        case d: JdbcExtendedDialect =>
          connProperties.putAll(d.extraDriverProperties(isLoner))
        case _ =>
      }
    }
    JdbcUtils.createConnection(url, connProperties)
  }

  def getConnector(id: String, driver: String, dialect: JdbcDialect,
      poolProps: Map[String, String], connProps: Properties,
      hikariCP: Boolean): () => Connection = {
    () => {
      ConnectionPool.getPoolConnection(id, Some(driver), dialect,
        poolProps, connProps, hikariCP)
    }
  }


  def getConnectionType(url: String) = {
    JdbcDialects.get(url) match {
      case GemFireXDDialect => ConnectionType.Embedded
      case GemFireXDClientDialect =>   ConnectionType.Net
      case _ => ConnectionType.Unknown
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
        case d: DecimalType => java.sql.Types.DECIMAL
        case NullType => java.sql.Types.NULL
        case _ => throw new IllegalArgumentException(
          s"Can't translate to JDBC value for type $dataType")
      })
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
        throw new AnalysisException("JDBCAppendableRelation: Cannot resolve " +
            s"""column name "$col" among (${fieldMap.keys.mkString(", ")})""")
      ))
    })
  }

  def getInsertString(table: String, userSchema: StructType) = {
    val sb = new mutable.StringBuilder("INSERT INTO ")
    sb.append(table).append(" VALUES (")
    (1 until userSchema.length).foreach { _ =>
      sb.append("?,")
    }
    sb.append("?)").toString()
  }

  def getInsertStringWithColumnName(table: String, rddSchema: StructType) = {
    val sb = new StringBuilder(s"INSERT INTO $table (")
    val schemaFields = rddSchema.fields
    (0 until (schemaFields.length - 1)).foreach { i =>
      sb.append(schemaFields(i).fieldName).append(',')
    }
    sb.append(schemaFields(schemaFields.length - 1).fieldName).append(") ")
    sb.append(" VALUES (")

    (1 until rddSchema.length).foreach { _ =>
      sb.append("?,")
    }
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
          case d: DecimalType =>
            row(col) match {
              case d: Decimal => stmt.setBigDecimal(col + 1, d.toJavaBigDecimal)
              case bd: java.math.BigDecimal => stmt.setBigDecimal(col + 1, bd)
              case s: String => stmt.setString(col + 1, s)
              case o => stmt.setObject(col + 1, o)
            }
          case _ => stmt.setObject(col + 1, row(col))
        }
      } else {
        stmt.setNull(col + 1, ExternalStoreUtils.getJDBCType(dialect, dataType))
      }
      col += 1
    }
  }
}

object ConnectionType extends Enumeration {
  type ConnectionType = Value
  val Embedded, Net, Unknown = Value
}
