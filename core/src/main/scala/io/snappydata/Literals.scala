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
package io.snappydata

import java.util.Properties

import org.apache.spark.SparkConf

/**
 * Constant names suggested per naming convention
 * http://docs.scala-lang.org/style/naming-conventions.html
 *
 * we decided to use upper case with underscore word separator.
 */
object Constant {

  val DEFAULT_EMBEDDED_URL = "jdbc:snappydata:"

  val SNAPPY_URL_PREFIX = "snappydata://"

  val JDBC_URL_PREFIX = "snappydata://"

  val JDBC_EMBEDDED_DRIVER = "com.pivotal.gemfirexd.jdbc.EmbeddedDriver"

  val JDBC_CLIENT_DRIVER = "com.pivotal.gemfirexd.jdbc.ClientDriver"

  val PROPERTY_PREFIX = "snappydata."

  val STORE_PROPERTY_PREFIX = s"${PROPERTY_PREFIX}store."

  val SPARK_PREFIX = "spark."

  val SPARK_SNAPPY_PREFIX = SPARK_PREFIX + PROPERTY_PREFIX

  val SPARK_STORE_PREFIX = SPARK_PREFIX + STORE_PROPERTY_PREFIX

  private[snappydata] val JOBSERVER_PROPERTY_PREFIX = "jobserver."

  val DEFAULT_SCHEMA = "APP"

  val DEFAULT_CONFIDENCE: Double = 0.95

  val DEFAULT_ERROR: Double = 0.2

  val DEFAULT_BEHAVIOR: String = "DEFAULT_BEHAVIOR"

  val COLUMN_MIN_BATCH_SIZE: Int = 200

  val DEFAULT_USE_HIKARICP = false

  // Interval in ms  to run the SnappyAnalyticsService
  val DEFAULT_CALC_TABLE_SIZE_SERVICE_INTERVAL: Long = 10000

  // Internal Column table store schema
  final val INTERNAL_SCHEMA_NAME = "SNAPPYSYS_INTERNAL"

  // Internal Column table store suffix
  final val SHADOW_TABLE_SUFFIX = "_COLUMN_STORE_"

  // Property to Specify whether zeppelin interpreter should be started
  // with leadnode
  val ENABLE_ZEPPELIN_INTERPRETER = "zeppelin.interpreter.enable"

  // Property to specify the port on which zeppelin interpreter
  // should be started
  val ZEPPELIN_INTERPRETER_PORT = "zeppelin.interpreter.port"

  val DEFAULT_CACHE_TIMEOUT_SECS = 10

  val CHAR_TYPE_BASE_PROP = "base"

  val CHAR_TYPE_SIZE_PROP = "size"

  val MAX_VARCHAR_SIZE = 32672

  // System property to tell the system whether the String type columns
  // should be considered as clob or not
  val STRING_AS_CLOB_PROP = "spark-string-as-clob"
}

/**
 * Property names should be as per naming convention
 * http://docs.scala-lang.org/style/naming-conventions.html
 * i.e. upper camel case.
 */
object Property extends Enumeration {

  final class ValueAlt(name: String, altName: String)
      extends Property.Val(name) {

    def getOption(conf: SparkConf): Option[String] = if (altName == null) {
      conf.getOption(name)
    } else {
      conf.getOption(name) match {
        case s: Some[String] => // check if altName also present and fail if so
          if (conf.contains(altName)) {
            throw new IllegalArgumentException(
              s"Both $name and $altName configured. Only one should be set.")
          } else s
        case None => conf.getOption(altName)
      }
    }

    def getProperty(properties: Properties): String = if (altName == null) {
      properties.getProperty(name)
    } else {
      val v = properties.getProperty(name)
      if (v != null) {
        // check if altName also present and fail if so
        if (properties.getProperty(altName) != null) {
          throw new IllegalArgumentException(
            s"Both $name and $altName specified. Only one should be set.")
        }
        v
      } else properties.getProperty(altName)
    }

    def apply(): String = name

    def unapply(key: String): Boolean = name.equals(key) ||
        (altName != null && altName.equals(key))

    override def toString(): String =
      if (altName == null) name else name + '/' + altName
  }

  type Type = ValueAlt

  protected final def Val(name: String): ValueAlt =
    new ValueAlt(name, null)

  protected final def Val(name: String, prefix: String): ValueAlt =
    new ValueAlt(name, prefix + name)

  val Locators = Val(s"${Constant.STORE_PROPERTY_PREFIX}locators",
    Constant.SPARK_PREFIX)

  val McastPort = Val(s"${Constant.STORE_PROPERTY_PREFIX}mcast-port",
    Constant.SPARK_PREFIX)

  val JobserverEnabled = Val(s"${Constant.JOBSERVER_PROPERTY_PREFIX}enabled")

  val JobserverConfigFile =
    Val(s"${Constant.JOBSERVER_PROPERTY_PREFIX}configFile")

  val Embedded = Val(s"${Constant.PROPERTY_PREFIX}embedded",
    Constant.SPARK_PREFIX)

  val MetastoreDBURL = Val(s"${Constant.PROPERTY_PREFIX}metastore-db-url",
    Constant.SPARK_PREFIX)

  val MetastoreDriver = Val(s"${Constant.PROPERTY_PREFIX}metastore-db-driver",
    Constant.SPARK_PREFIX)

  val LocalCacheTimeout = Val(s"${Constant.SPARK_PREFIX}sql.cacheTimeout")
}

/**
 * SQL query hints as interpreted by the SnappyData SQL parser. The format
 * mirrors closely the format used by Hive,Oracle query hints with a comment
 * followed immediately by a '+' and then "key(value)" for the hint. Example:
 * <p>
 * SELECT * /*+ hint(value) */ FROM t1
 */
object QueryHint extends Enumeration {

  type Type = Value

  /**
   * Query hint for SQL queries to serialize complex types (ARRAY, MAP, STRUCT)
   * as CLOBs (their string representation) for routed JDBC/ODBC queries rather
   * than as serialized blobs to display better in external tools.
   * <p>
   * Possible values are 'true/1' or 'false/0'
   * <p>
   * Example:<br>
   * SELECT * FROM t1 --+ complexTypeAsClob(1)
   */
  val ComplexTypeAsClob = Value("complexTypeAsClob")

  /**
   * Query hint for SQL queries to serialize STRING type as CLOB rather than
   * as VARCHAR.
   * <p>
   * Possible values are valid column names in the tables/schema. Multiple
   * column names to be comma separated.
   * One can also provide '*' for serializing all the STRING columns as CLOB.
   * <p>
   * Example:<br>
   * SELECT id, name, addr, medical_history FROM t1 --+ columnsAsClob(addr)
   * SELECT id, name, addr, medical_history FROM t1 --+ columnsAsClob(*)
   */
  val ColumnsAsClob = Value("columnsAsClob")
}
