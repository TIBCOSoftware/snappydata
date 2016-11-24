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

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConfigEntry

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

  val DEFAULT_USE_HIKARICP = true

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

  val DEFAULT_SERIALIZER = "org.apache.spark.serializer.PooledKryoSerializer"

  // LZF gives best balance between speed and compression ratio
  // (LZ4 JNI is slightly faster but LZF compression is higher)
  val DEFAULT_CODEC = "lz4"

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

  case class ValueAlt(name: String, altName: String,
      configEntry: SQLConfigEntry) extends Property.Val(name) {

    def defaultValue[T]: Option[T] = configEntry.defaultValue[T]

    def defaultValueString: String = configEntry.defaultValueString

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

  case class SQLValue(name: String, configEntry: SQLConfigEntry)
      extends Property.Val(name) {

    def defaultValue[T]: Option[T] = configEntry.defaultValue[T]

    def defaultValueString: String = configEntry.defaultValueString

    override def toString(): String = name
  }

  type Type = ValueAlt

  type SQLType = SQLValue

  protected final def Val[T: ClassTag](name: String, doc: String,
      defaultValue: Option[T], prefix: String = null,
      isPublic: Boolean = true): ValueAlt = {
    ValueAlt(name, if (prefix == null) null else prefix + name,
      SQLConfigEntry.sparkConf(name, doc, defaultValue, isPublic))
  }

  protected final def SQLVal[T: ClassTag](name: String, doc: String,
      defaultValue: Option[T], isPublic: Boolean = true): SQLValue = {
    SQLValue(name, SQLConfigEntry(name, doc, defaultValue, isPublic))
  }

  val Locators = Val[String](s"${Constant.STORE_PROPERTY_PREFIX}locators",
    "The list of locators as comma-separated host:port values that have been " +
        "configured in the SnappyData cluster.", None, Constant.SPARK_PREFIX)

  val McastPort = Val[Int](s"${Constant.STORE_PROPERTY_PREFIX}mcast-port",
    "[Deprecated] The multicast port configured in the SnappyData cluster " +
        "when locators are not being used. This mode is no longer supported.",
    None, Constant.SPARK_PREFIX)

  val JobserverEnabled = Val(s"${Constant.JOBSERVER_PROPERTY_PREFIX}enabled",
    "If true then REST API access via Spark jobserver will be available in " +
        "the SnappyData cluster", Some(true), prefix = null, isPublic = false)

  val Embedded = Val(s"${Constant.PROPERTY_PREFIX}embedded",
    "Enabled in SnappyData embedded cluster and disabled for other " +
        "deployments.", Some(true), Constant.SPARK_PREFIX, isPublic = false)

  val MetastoreDBURL = Val[String](s"${Constant.PROPERTY_PREFIX}metastore-db-url",
    "An explicit JDBC URL to use for external meta-data storage. " +
        "Normally this is set to use the SnappyData store by default and " +
        "should not be touched unless there are special requirements. " +
        "Use with caution since an incorrect configuration can result in " +
        "loss of entire meta-data (and thus data).", None, Constant.SPARK_PREFIX)

  val MetastoreDriver = Val[String](s"${Constant.PROPERTY_PREFIX}metastore-db-driver",
    s"Explicit JDBC driver class for ${MetastoreDBURL()} setting.",
    None, Constant.SPARK_PREFIX)

  val ColumnBatchSizeMb = SQLVal(s"${Constant.PROPERTY_PREFIX}columnBatchSizeMb",
    "The default size of blocks to use for storage in SnappyData column " +
        "store. When inserting data into the column storage this is " +
        "the unit (in MB) that will be used to split the data into chunks " +
        "for efficient storage and retrieval.", Some(32))
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
   * as CLOBs in JSON format for routed JDBC/ODBC queries rather
   * than as serialized blobs to display better in external tools.
   * <p>
   * Possible values are 'true/1' or 'false/0'
   * <p>
   * Example:<br>
   * SELECT * FROM t1 --+ complexTypeAsJson(1)
   */
  val ComplexTypeAsJson = Value("complexTypeAsJson")

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
