/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import com.gemstone.gemfire.internal.shared.SystemProperties

/**
 * Constant names suggested per naming convention
 * http://docs.scala-lang.org/style/naming-conventions.html
 *
 * we decided to use upper case with underscore word separator.
 */
object Constant {

  val DEFAULT_EMBEDDED_URL = "jdbc:snappydata:"

  val DEFAULT_THIN_CLIENT_URL = "jdbc:snappydata://"

  val POOLED_THIN_CLIENT_URL = "jdbc:snappydata:pool://"

  val SNAPPY_URL_PREFIX = "snappydata://"

  val JDBC_EMBEDDED_DRIVER = "io.snappydata.jdbc.EmbeddedDriver"

  val JDBC_CLIENT_DRIVER = "io.snappydata.jdbc.ClientDriver"

  val JDBC_CLIENT_POOL_DRIVER = "io.snappydata.jdbc.ClientPoolDriver"

  val PROPERTY_PREFIX = "snappydata."

  val STORE_PROPERTY_PREFIX: String = SystemProperties.SNAPPY_PREFIX

  val SPARK_PREFIX = "spark."

  val SPARK_SNAPPY_PREFIX: String = SPARK_PREFIX + PROPERTY_PREFIX

  val SPARK_STORE_PREFIX: String = SPARK_PREFIX + STORE_PROPERTY_PREFIX

  val JOBSERVER_PROPERTY_PREFIX = "jobserver."

  val CONNECTION_PROPERTY: String = s"${PROPERTY_PREFIX}connection"

  val DEFAULT_SCHEMA = "APP"

  val DEFAULT_CONFIDENCE: Double = 0.95d

  val DEFAULT_ERROR: Double = 0.2d

  val DEFAULT_BEHAVIOR: String = "DEFAULT_BEHAVIOR"
  val BEHAVIOR_RUN_ON_FULL_TABLE = "RUN_ON_FULL_TABLE"
  val BEHAVIOR_DO_NOTHING = "DO_NOTHING"
  val BEHAVIOR_LOCAL_OMIT = "LOCAL_OMIT"
  val BEHAVIOR_STRICT = "STRICT"
  val BEHAVIOR_PARTIAL_RUN_ON_BASE_TABLE = "PARTIAL_RUN_ON_BASE_TABLE"

  val keyBypassSampleOperator = "aqp.debug.byPassSampleOperator"
  val defaultBehaviorAsDO_NOTHING = "spark.sql.aqp.defaultBehaviorAsDO_NOTHING"

  val DEFAULT_USE_HIKARICP = false

  // Interval in ms  to run the SnappyAnalyticsService
  val DEFAULT_CALC_TABLE_SIZE_SERVICE_INTERVAL: Long = 5000

  /** Special property to trigger authentication by SnappyConf. */
  val TRIGGER_AUTHENTICATION = "snappydata.auth.trigger"

  // Internal Column table store schema
  final val SHADOW_SCHEMA_NAME = SystemProperties.SHADOW_SCHEMA_NAME

  // Internal Column table store suffix
  final val SHADOW_TABLE_SUFFIX = SystemProperties.SHADOW_TABLE_SUFFIX

  final val SHADOW_SCHEMA_SEPARATOR = SystemProperties.SHADOW_SCHEMA_SEPARATOR

  final val SHADOW_SCHEMA_NAME_WITH_PREFIX: String = "." + SHADOW_SCHEMA_NAME

  final val SHADOW_SCHEMA_NAME_WITH_SEPARATOR =
    SystemProperties.SHADOW_SCHEMA_NAME_WITH_SEPARATOR

  final val COLUMN_TABLE_INDEX_PREFIX = "snappysys_index____"

  // Property to Specify whether zeppelin interpreter should be started
  // with leadnode
  val ENABLE_ZEPPELIN_INTERPRETER = "zeppelin.interpreter.enable"

  // Property to specify the port on which zeppelin interpreter
  // should be started
  val ZEPPELIN_INTERPRETER_PORT = "zeppelin.interpreter.port"

  // System property for minimum size of buffer to consider for compression.
  val COMPRESSION_MIN_SIZE: String = PROPERTY_PREFIX + "compression.minSize"

  val LOW_LATENCY_POOL: String = "lowlatency"

  val CHAR_TYPE_BASE_PROP = "base"

  val CHAR_TYPE_SIZE_PROP = "size"

  val MAX_VARCHAR_SIZE = 32672

  val MAX_CHAR_SIZE = 254

  // allowed values for QueryHint.JoinType
  val JOIN_TYPE_BROADCAST = "broadcast"
  val JOIN_TYPE_HASH = "hash"
  val JOIN_TYPE_SORT = "sort"
  val ALLOWED_JOIN_TYPE_HINTS: List[String] =
    List(JOIN_TYPE_BROADCAST, JOIN_TYPE_HASH, JOIN_TYPE_SORT)

  /**
   * Limit the maximum number of rows in a column batch (applied before
   * ColumnBatchSize property).
   */
  val MAX_ROWS_IN_BATCH = 200000

  val DEFAULT_SERIALIZER = "org.apache.spark.serializer.PooledKryoSerializer"

  // LZ4 JNI version is the fastest one but LZF gives best balance between
  // speed and compression ratio having higher compression ration than LZ4.
  // But the JNI version means no warmup time which helps for short jobs.
  // Also LZF has no direct ByteBuffer API so is quite a bit slower for off-heap.
  val DEFAULT_CODEC: String = SystemProperties.SNAPPY_DEFAULT_COMPRESSION_CODEC

  // System property to tell the system whether the String type columns
  // should be considered as clob or not in JDBC/ODBC SQL queries
  val STRING_AS_CLOB_PROP = "spark.sql.stringAsClob"

  val CHANGEABLE_JAR_NAME = "SNAPPY_CHANGEABLE_JAR_NAME"

  val RESERVOIR_AS_REGION = "spark.sql.aqp.reservoirAsRegion"

  val EXTERNAL_TABLE_RLS_ENABLE_KEY = "rls.enabled"

  val COMPLEX_TYPE_AS_JSON_HINT = "complexTypeAsJson"

  val COMPLEX_TYPE_AS_JSON_DEFAULT = true
}
