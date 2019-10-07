/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.execution

import java.sql.Connection
import java.util.Properties
import javax.sql.DataSource

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.pivotal.gemfirexd.Attribute
import com.zaxxer.hikari.util.PropertyElf
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.tomcat.jdbc.pool.{PoolProperties, DataSource => TDataSource}

import org.apache.spark.sql.SnappyDataBaseDialect
import org.apache.spark.sql.jdbc.JdbcDialect

/**
 * A global way to obtain a pooled DataSource with a given set of
 * pool and connection properties.
 *
 * Supports Tomcat-JDBC pool and HikariCP.
 */
object ConnectionPool {

  /**
   * Type of the key used to search for matching pool. Tuple of pool properties,
   * connection properties, and whether pool uses Tomcat pool or HikariCP.
   */
  private[this] type PoolKey = (Properties, Properties, Boolean)

  /**
   * Map of ID to corresponding pool DataSources. Using Java's
   * ConcurrentHashMap for lock-free reads. No concurrency required
   * only lock-free reads so keeping it at minimum.
   */
  private[this] val idToPoolMap = new java.util.concurrent.ConcurrentHashMap[
      (String, String), (DataSource, PoolKey)](8, 0.75f, 1)

  /**
   * Reference counted pools which will share a pool across IDs
   * where possible i.e. when all the pool and connection properties match.
   */
  private[this] val pools = mutable.Map[PoolKey,
      (DataSource, mutable.Set[String])]()

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
   * @param id              an ID for a pool that will shared by all requests against the
   *                        same id (e.g. the table name can be the ID for an external table)
   * @param props           map of pool properties to their values; the key can be either
   *                        `PoolProperty.Type` or a string (as in Tomcat or Hikari)
   * @param connectionProps set of any additional connection properties
   * @param hikariCP        if true then use HikariCP else Tomcat-JDBC pool
   *                        implementation; default is false i.e. Tomcat pool
   */
  def getPoolDataSource(id: String, props: Map[String, String],
      connectionProps: Properties, hikariCP: Boolean): DataSource = {
    // fast lock-free path first (the usual case)
    val username = props.getOrElse(Attribute.USERNAME_ALT_ATTR.toLowerCase, "")
    val lookupKey = id -> username
    val dsKey = idToPoolMap.get(lookupKey)
    if (dsKey != null) {
      dsKey._1
    } else pools.synchronized {
      // double check after the global lock
      val dsKey = idToPoolMap.get(lookupKey)
      if (dsKey != null) {
        dsKey._1
      } else {
        // search if there is already an existing pool with same properties
        val poolProps = new Properties()
        for ((k, v) <- props) poolProps.setProperty(k, v)
        val poolKey: PoolKey = (poolProps, connectionProps, hikariCP)
        pools.get(poolKey) match {
          case Some((newDS, ids)) =>
            ids += id
            val err = idToPoolMap.putIfAbsent(lookupKey, (newDS, poolKey))
            require(err == null, s"unexpected existing pool for $id: $err")
            newDS
          case None =>
            // create new pool
            val newDS: DataSource = if (hikariCP) {
              val hconf = new HikariConfig(poolProps)
              if (connectionProps != null) {
                hconf.setDataSourceProperties(connectionProps)
              }
              new HikariDataSource(hconf)
            } else {
              val tconf = new PoolProperties()
              PropertyElf.setTargetFromProperties(tconf, poolProps)
              if (connectionProps != null) {
                tconf.setDbProperties(connectionProps)
              }
              tconf.setUseStatementFacade(false)
              new TDataSource(tconf)
            }
            pools(poolKey) = (newDS, mutable.Set(id))
            val err = idToPoolMap.putIfAbsent(lookupKey, (newDS, poolKey))
            require(err == null, s"unexpected existing pool for $id: $err")
            newDS
        }
      }
    }
  }

  /**
   * Utility method to get the connection from DataSource returned by
   * `getPoolDataSource`.
   *
   * @see getPoolDataSource
   */
  def getPoolConnection(id: String, dialect: JdbcDialect,
      poolProps: Map[String, String], connProps: Properties,
      hikariCP: Boolean): Connection = {
    val ds = getPoolDataSource(id, poolProps, connProps, hikariCP)
    val conn = ds.getConnection
    dialect match {
      case _: SnappyDataBaseDialect =>
        conn.setTransactionIsolation(Connection.TRANSACTION_NONE)

      case _ =>
    }
    conn
  }

  private def removePoolKey(id: String,
      dsKey: (DataSource, PoolKey)): Boolean = {
    // we expect the ID to be present in pools
    val poolKey = dsKey._2
    val ids = pools(poolKey)._2
    ids -= id
    // clear the entire pool if all references have been removed
    if (ids.isEmpty) {
      pools -= poolKey
      if (poolKey._3) {
        dsKey._1.asInstanceOf[HikariDataSource].close()
      } else {
        dsKey._1.asInstanceOf[TDataSource].close(true)
      }
      true
    } else false
  }

  /**
   * Remove reference to the pool for given ID. Also will cleanup and remove
   * the pool if all the references have been removed.
   *
   * @return true if this was the last reference and entire pool was removed
   *         and false otherwise
   */
  def removePoolReference(id: String): Boolean = {
    val ids = idToPoolMap.asScala.keySet.filter(_._1.equalsIgnoreCase(id))
    ids.forall(id => removePoolReference(id._1, id._2))
  }

  def removePoolReference(id: String, userName: String): Boolean = {
    val dsKey = idToPoolMap.remove((id, userName))
    if (dsKey != null) pools.synchronized {
      removePoolKey(id, dsKey)
    } else false
  }

  /** To be invoked only on SparkContext stop or from tests (or LDAP refresh). */
  def clear(): Unit = pools.synchronized {
    idToPoolMap.asScala.foreach {
      case (id, dsKey) => removePoolKey(id._1, dsKey)
    }
    pools.clear()
    idToPoolMap.clear()
  }
}
