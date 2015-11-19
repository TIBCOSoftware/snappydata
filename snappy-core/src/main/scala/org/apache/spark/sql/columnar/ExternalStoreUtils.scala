package org.apache.spark.sql.columnar

import java.sql.{PreparedStatement, Connection}
import java.util.Properties

import org.apache.spark.sql.{Row, AnalysisException}
import org.apache.spark.sql.types._

import scala.collection.mutable
import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.row.{GemFireXDClientDialect, GemFireXDDialect}
import org.apache.spark.sql.store.StoreProperties

/**
 * Utility methods used by external storage layers.
 */
private[sql] object ExternalStoreUtils extends Logging {

  def getAllPoolProperties(url: String, driver: String,
      poolProps: Map[String, String], hikariCP: Boolean) = {
    val urlProp = if (hikariCP) "jdbcUrl" else "url"
    val driverClassProp = "driverClassName"
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

  def getDriver(url : String): Option[String] = {
    val dialect = JdbcDialects.get(url)
    dialect match {
      case  GemFireXDDialect => Option("com.pivotal.gemfirexd.jdbc.EmbeddedDriver")
      case  GemFireXDClientDialect => Option("com.pivotal.gemfirexd.jdbc.ClientDriver")
      case _=> Option(DriverRegistry.getDriverClassName(url))
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

  def validateAndGetAllProps(sc : SparkContext, options: Map[String, String]) = {
    val parameters = new mutable.HashMap[String, String]
    parameters ++= options


    val url = parameters.remove("url").getOrElse {
       StoreProperties.defaultStoreURL(sc)
    }

    val driver = parameters.remove("driver").orElse(getDriver(url))

    driver.foreach(DriverRegistry.register)

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
    connProps.setProperty("route-query", "false")
    val allPoolProps = getAllPoolProperties(url, driver.get,
      poolProps, hikariCP)
    (url, driver.get, allPoolProps, connProps, hikariCP)
  }

  def getPoolConnection(id: String, driver: Option[String],
      poolProps: Map[String, String], connProps: Properties,
      hikariCP: Boolean): Connection = {
    try {
      if (driver.isDefined) DriverRegistry.register(driver.get)
    } catch {
      case cnfe: ClassNotFoundException => throw new IllegalArgumentException(
        s"Couldn't find driver class $driver", cnfe)
    }
    ConnectionPool.getPoolConnection(id, poolProps, connProps, hikariCP)
  }

  def getConnection(url: String, connProperties: Properties) = {
    connProperties.remove("poolProperties")
    val con = JdbcUtils.createConnection(url, connProperties)
    con.setTransactionIsolation(Connection.TRANSACTION_NONE)
    con
    //DriverManager.getConnection(url)
  }

  def getConnectionType(url: String) = {
    JdbcDialects.get(url) match {
      case GemFireXDDialect => ConnectionType.Embedded
      case GemFireXDClientDialect => ConnectionType.Net
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
    (1 until userSchema.length).foreach(sb.append("?,"))
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
