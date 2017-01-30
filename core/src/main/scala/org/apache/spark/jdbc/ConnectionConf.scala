/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.jdbc

import java.io.{IOException, ObjectOutputStream}

import scala.StringBuilder

import org.apache.spark.sql.{SnappySession, SnappyContext}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.util.Utils

import scala.collection.mutable

class ConnectionConf(val connProps: ConnectionProperties) extends Serializable {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    oos.defaultWriteObject()
  }
}

class ConnectionConfBuilder(session: SnappySession) {

  val URL = "url"
  val DRIVER = "driver"
  val poolProvider = "poolimpl"

  @transient private val connSettings = new mutable.HashMap[String, String]()

  @transient private val poolSettings = new mutable.HashMap[String, String]()

  /**
   * Set the URL for connection. For SnappyData you can skip setting this. SnappyData will
   * automatically set the URL based on the cluster
   * @param url URL for JDBC connection
   */
  def setURL(url: String) = {
    connSettings.put(URL, url)
    this
  }

  /**
   * Set the driver for the connection. For SnappyData no need to set this. SnappyData will
   * automatically set the driver based on the cluster
   * @param driver driver for the connection
   */
  def setDriver(driver: String) = {
    connSettings.put(DRIVER, driver)
    this
  }

  /**
   * Snappy supports two ConnectionPool implementation "Tomcat" and "Hikari".
   * Set either of the two . Default is Tomcat.
   * @param provider provider name
   */
  def setPoolProvider(provider: String) = {
    connSettings.put(poolProvider, provider)
    this
  }

  /**
   * Sets a property for a pool.
   * For detailed list for Hikari refer https://github.com/brettwooldridge/HikariCP/wiki/Configuration
   * For detailed list for Tomcat refer https://people.apache.org/~fhanik/jdbc-pool/jdbc-pool.html
   *
   * Even if you don't mention any properties it defaults it to some sensible values
   *
   * @param prop property name
   * @param value property value
   */
  def setPoolConf(prop: String, value: String) = {
    poolSettings.put(prop, value)
    this
  }

  /**
   * Sets properties for a pool.
   * For detailed list for Hikari refer https://github.com/brettwooldridge/HikariCP/wiki/Configuration
   * For detailed list for Tomcat refer https://people.apache.org/~fhanik/jdbc-pool/jdbc-pool.html
   *
   * Even if you don't mention any properties it defaults it to some sensible values
   *
   * @param props map of key-value
   */
  def setPoolConfs(props: Map[String, String]) = {
    if (props != null && !props.isEmpty) {
      props.map(e => poolSettings.put(e._1, e._2))
    }
    this
  }

  /**
   * Sets any additional information in connection setting
   * @param prop property name
   * @param value property value
   */
  def setConf(prop: String, value: String) = {
    connSettings.put(prop, value)
    this
  }

  /**
   * Prepares the ConnectionConf to be usable to obtain a connection.
   * See ConnectionUtil for various APIs to get a connection from a
   * ConnectionConf
   */
  def build(): ConnectionConf = {
    if(poolSettings.size > 0){
      val sb = new StringBuilder()
      val poolProperties = poolSettings.map( x => (s"${x._1}=${x._2}")).mkString(",")
      connSettings.put("poolProperties", poolProperties)
    }

    val connProps = ExternalStoreUtils.validateAndGetAllProps(Some(session.sparkContext),
      new CaseInsensitiveMutableHashMap(connSettings))
    new ConnectionConf(connProps)
  }
}