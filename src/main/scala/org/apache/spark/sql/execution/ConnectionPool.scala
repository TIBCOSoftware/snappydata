package org.apache.spark.sql.execution

import java.util.Properties
import javax.sql.DataSource

import scala.collection.mutable

import com.zaxxer.hikari.{HikariConfig => HConf, HikariDataSource => HDataSource}
import org.apache.spark.sql.collection.Utils
import org.apache.tomcat.jdbc.pool.{DataSource => TDataSource, PoolConfiguration => TConf, PoolProperties}

/**
 * A global way to obtain a pooled DataSource with a given set of
 * pool and connection properties.
 *
 * Supports Tomcat-JDBC pool and HikariCP.
 */
object ConnectionPool {

  /**
   * Type of the key used to search for matching pool. Tuple of pool properties,
   * connection properties, and whether pool uses Tomcat pool or HikariCP pool.
   */
  private[this] type PoolKey = (Map[PoolProperty.Type, String],
      Properties, Boolean)

  /**
   * Map of ID to corresponding pool DataSources. Using Java's
   * ConcurrentHashMap for lock-free reads. No concurrency required
   * so keeping it at minimum.
   */
  private[this] val idToPoolMap = new java.util.concurrent.ConcurrentHashMap[
      String, (DataSource, PoolKey)](8, 0.75f, 1)

  /**
   * Reference counted pools which will share a pool across IDs
   * where possible i.e. when all the pool and connection properties match.
   */
  private[this] val pools = mutable.Map[PoolKey,
      (DataSource, mutable.Set[String])]()

  private[this] val EMPTY_PROPS = new Properties()

  /**
   * Get a pooled DataSource for a given ID, pool properties and connection
   * properties. Currently two pool implementations are supported namely
   * Tomcat-JDBC pool and HikariCP implementation which is selected by an
   * optional boolean argument (default is false i.e. tomcat pool).
   *
   * It is assumed and required that the set of pool properties, connection
   * properties (and whether tomcat or hikari pool is to be used) are provided
   * as identical for the same ID.
   *
   * @param id an ID for a pool that will shared by all requests against the
   *           same id (e.g. the table name can be the idle for external table)
   * @param props map of pool properties to their values; the key can be either
   *              `PoolProperty.Type` or a string (as in Tomcat or Hikari)
   * @param connectionProps set of any additional connection properties
   * @param hikariCP if true then use HikariCP else Tomcat-JDBC pool
   *                 implementation; default is false i.e. Tomcat pool
   */
  def getPoolDataSource(id: String, props: Map[_, String],
      connectionProps: Properties = EMPTY_PROPS,
      hikariCP: Boolean = false): DataSource = {
    // fast lock-free path first (the usual case)
    val dsKey = idToPoolMap.get(id)
    if (dsKey != null) {
      dsKey._1
    } else pools.synchronized {
      // double check after the global lock
      val dsKey = idToPoolMap.get(id)
      if (dsKey != null) {
        dsKey._1
      } else {
        // search if there is already an existing pool with same properties
        val poolProps = props.map {
          case (k: PoolProperty.Type, v) => (k, v)
          case (k, v) => (PoolProperty.valueOf(k.toString), v)
        }
        val poolKey: PoolKey = (poolProps, connectionProps, hikariCP)
        pools.get(poolKey) match {
          case Some((newDS, ids)) =>
            ids += id
            val err = idToPoolMap.putIfAbsent(id, (newDS, poolKey))
            require(err == null, s"unexpected existing pool for $id: $err")
            newDS
          case None =>
            // create new pool
            val newDS: DataSource = if (hikariCP) {
              val hconf = new HConf
              for ((prop, value) <- poolProps) {
                if (prop.hikariMethod == null) {
                  throw new IllegalArgumentException("ConnectionPool: " +
                      s"unknown property '$prop' for Hikari connection pool")
                }
                prop.hikariMethod(hconf, value)
              }
              if (connectionProps != null) {
                hconf.setDataSourceProperties(connectionProps)
              }
              new HDataSource(hconf)
            } else {
              val tconf = new PoolProperties
              for ((prop, value) <- poolProps) {
                if (prop.tomcatMethod == null) {
                  throw new IllegalArgumentException("ConnectionPool: " +
                      s"unknown property '$prop' for Tomcat JDBC pool")
                }
                prop.tomcatMethod(tconf, value)
              }
              if (connectionProps != null) {
                tconf.setDbProperties(connectionProps)
              }
              new TDataSource(tconf)
            }
            pools(poolKey) = (newDS, mutable.Set(id))
            val err = idToPoolMap.putIfAbsent(id, (newDS, poolKey))
            require(err == null, s"unexpected existing pool for $id: $err")
            newDS
        }
      }
    }
  }

  /**
   * Remove reference to the pool for given ID. Also will cleanup and remove
   * the pool if all the references have been removed.
   *
   * @return true if this was the last reference and entire pool was removed
   *         and false otherwise
   */
  def removePoolReference(id: String): Boolean = {
    val dsKey = idToPoolMap.remove(id)
    if (dsKey != null) pools.synchronized {
      // we expect the ID to be present in pools
      val poolKey = dsKey._2
      val ids = pools(poolKey)._2
      ids -= id
      // clear the entire pool if all references have been removed
      if (ids.isEmpty) {
        pools -= poolKey
        if (poolKey._3) {
          dsKey._1.asInstanceOf[HDataSource].shutdown()
        } else {
          dsKey._1.asInstanceOf[TDataSource].close(true)
        }
        true
      } else false
    } else false
  }
}

object PoolProperty extends Enumeration {

  private[this] final val nameMap = new mutable.HashMap[String, Type]
  private[this] final val NO_NAMES = Seq.empty[String]

  final class Type(i: Int, name: String,
      val tomcatMethod: (TConf, String) => Unit,
      val hikariMethod: (HConf, String) => Unit,
      alternateNames: Seq[String] = NO_NAMES) extends Val(i, name) {

    // since this is only populated statically, so no need of synchronization
    nameMap(name) = this // for fast lookup path
    nameMap(Utils.normalizeId(name)) = this
    alternateNames.foreach(id => nameMap(Utils.normalizeId(id)) = this)

    def this(name: String, tomcatMethod: (TConf, String) => Unit,
        hikariMethod: (HConf, String) => Unit,
        alternateNames: Seq[String]) =
      this(nextId, name, tomcatMethod, hikariMethod, alternateNames)
  }

  def Value(i: Int, name: String, tomcatMethod: (TConf, String) => Unit,
      hikariMethod: (HConf, String) => Unit,
      alternateNames: Seq[String]) =
    new Type(i, name, tomcatMethod, hikariMethod, alternateNames)

  def Value(name: String, tomcatMethod: (TConf, String) => Unit,
      hikariMethod: (HConf, String) => Unit,
      alternateNames: Seq[String] = NO_NAMES) =
    new Type(name, tomcatMethod, hikariMethod, alternateNames)

  def valueOf(name: String) = nameMap.getOrElse(name,
    nameMap.getOrElse(Utils.normalizeId(name),
      throw new IllegalArgumentException(s"unknown pool property $name")))

  // "primary" names for properties below are those used by Tomcat-JDBC pool,
  // though enumeration variable names themselves are chosen to be the more
  // "natural" one among the Tomcat-JDBC and Hikari pool property names

  val URL = Value("url", (tc: TConf, vs: String) => tc.setUrl(vs),
    (hc: HConf, vs: String) => hc.setJdbcUrl(vs),
    Seq("jdbcUrl"))
  val DriverClassName = Value("driverClassName",
    (tc: TConf, vs: String) => tc.setDriverClassName(vs),
    (hc: HConf, vs: String) => hc.setDriverClassName(vs),
    Seq("driverClass"))

  val UserName = Value("username",
    (tc: TConf, vs: String) => tc.setUsername(vs),
    (hc: HConf, vs: String) => hc.setUsername(vs),
    Seq("user"))
  val Password = Value("password",
    (tc: TConf, vs: String) => tc.setPassword(vs),
    (hc: HConf, vs: String) => hc.setPassword(vs))

  val MaxPoolSize = Value("maxActive",
    (tc: TConf, vs: String) => tc.setMaxActive(vs.toInt),
    (hc: HConf, vs: String) => hc.setMaximumPoolSize(vs.toInt),
    Seq("maxPoolSize", "maximumPoolSize"))
  val MinIdle = Value("minIdle",
    (tc: TConf, vs: String) => tc.setMinIdle(vs.toInt),
    (hc: HConf, vs: String) => hc.setMinimumIdle(vs.toInt),
    Seq("minimumIdle"))
  /**
   * Only in tomcat pool: maximum number of connections that should be
   * kept in the pool at all times
   */
  val MaxIdle = Value("maxIdle",
    (tc: TConf, vs: String) => tc.setMaxIdle(vs.toInt),
    null /* no equivalent in HikariCP */)

  val AutoCommit = Value("defaultAutoCommit",
    (tc: TConf, vs: String) => tc.setDefaultAutoCommit(vs.toBoolean),
    (hc: HConf, vs: String) => hc.setAutoCommit(vs.toBoolean),
    Seq("autoCommit"))
  val TransactionIsolation = Value("defaultTransactionIsolation",
    (tc: TConf, vs: String) => tc.setDefaultTransactionIsolation(vs.toInt),
    (hc: HConf, vs: String) => hc.setTransactionIsolation(vs),
    Seq("transactionIsolation"))
}
