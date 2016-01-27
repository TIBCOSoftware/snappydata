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
package org.apache.spark.sql.store

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.{DistributedRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.sql.collection.{MultiExecutorLocalPartition, Utils}
import org.apache.spark.sql.columnar.{ExternalStoreUtils, ConnectionProperties}
import org.apache.spark.sql.execution.datasources.DDLException
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, JDBCRDD}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SQLContext}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Logging, Partition, SparkContext}

/*/10/15.
  */
object StoreUtils extends Logging {

  val PARTITION_BY = ExternalStoreUtils.PARTITION_BY
  val REPLICATE = ExternalStoreUtils.REPLICATE
  val BUCKETS = ExternalStoreUtils.BUCKETS
  val COLOCATE_WITH = "COLOCATE_WITH"
  val REDUNDANCY = "REDUNDANCY"
  val RECOVERYDELAY = "RECOVERYDELAY"
  val MAXPARTSIZE = "MAXPARTSIZE"
  val EVICTION_BY = "EVICTION_BY"
  val PERSISTENT = "PERSISTENT"
  val SERVER_GROUPS = "SERVER_GROUPS"
  val OFFHEAP = "OFFHEAP"
  val EXPIRE = "EXPIRE"
  val OVERFLOW = "OVERFLOW"

  val GEM_PARTITION_BY = "PARTITION BY"
  val GEM_BUCKETS = "BUCKETS"
  val GEM_COLOCATE_WITH = "COLOCATE WITH"
  val GEM_REDUNDANCY = "REDUNDANCY"
  val GEM_REPLICATE = "REPLICATE"
  val GEM_RECOVERYDELAY = "RECOVERYDELAY"
  val GEM_MAXPARTSIZE = "MAXPARTSIZE"
  val GEM_EVICTION_BY = "EVICTION BY"
  val GEM_PERSISTENT = "PERSISTENT"
  val GEM_SERVER_GROUPS = "SERVER GROUPS"
  val GEM_OFFHEAP = "OFFHEAP"
  val GEM_EXPIRE = "EXPIRE"
  val GEM_OVERFLOW = "EVICTACTION OVERFLOW"
  val GEM_HEAPPERCENT = "EVICTION BY LRUHEAPPERCENT "
  val PRIMARY_KEY = "PRIMARY KEY"
  val LRUCOUNT = "LRUCOUNT"

  val ddlOptions = Seq(PARTITION_BY, REPLICATE, BUCKETS, COLOCATE_WITH,
    REDUNDANCY, RECOVERYDELAY, MAXPARTSIZE, EVICTION_BY,
    PERSISTENT, SERVER_GROUPS, OFFHEAP, EXPIRE, OVERFLOW)

  val EMPTY_STRING = ""

  val SHADOW_COLUMN_NAME = "rowid"

  val SHADOW_COLUMN = s"$SHADOW_COLUMN_NAME bigint generated always as identity"


  def lookupName(tableName: String, schema: String): String = {
    val lookupName = {
      if (tableName.indexOf('.') <= 0) {
        schema + '.' + tableName
      } else tableName
    }
    lookupName
  }

  def getPartitionsPartitionedTable(sc: SparkContext,
      tableName: String, schema: String,
      blockMap: Map[InternalDistributedMember, BlockManagerId]): Array[Partition] = {

    val resolvedName = lookupName(tableName, schema)
    val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]
    val numPartitions = region.getTotalNumberOfBuckets
    val partitions = new Array[Partition](numPartitions)

    for (p <- 0 until numPartitions) {
      val distMembers = region.getRegionAdvisor.getBucketOwners(p).asScala
      val prefNodes = distMembers.map(
        m => blockMap.get(m)
      )
      val prefNodeSeq = prefNodes.map(_.get).toSeq
      partitions(p) = new MultiExecutorLocalPartition(p, prefNodeSeq)
    }
    partitions
  }

  def getPartitionsReplicatedTable(sc: SparkContext,
      tableName: String, schema: String,
      blockMap: Map[InternalDistributedMember, BlockManagerId]): Array[Partition] = {

    val resolvedName = lookupName(tableName, schema)
    val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[DistributedRegion]
    val numPartitions = 1
    val partitions = new Array[Partition](numPartitions)

    val regionMembers = if (Utils.isLoner(sc)) {
      Set(Misc.getGemFireCache.getDistributedSystem.getDistributedMember)
    } else {
      region.getDistributionAdvisor().adviseInitializedReplicates().asScala
    }
    val prefNodes = regionMembers.map(v => blockMap(v)).toSeq
    partitions(0) = new MultiExecutorLocalPartition(0, prefNodes)
    partitions
  }

  def initStore(sqlContext: SQLContext,
      table: String,
      schema: Option[StructType],
      partitions:Integer,
      connProperties:ConnectionProperties):
      Map[InternalDistributedMember, BlockManagerId] = {
    // TODO for SnappyCluster manager optimize this . Rather than calling this
    val blockMap = new StoreInitRDD(sqlContext,table,
      schema , partitions , connProperties).collect()
    blockMap.toMap
  }

  def appendClause(sb: mutable.StringBuilder, getClause: () => String): Unit = {
    val clause = getClause.apply()
    if (!clause.isEmpty) {
      sb.append(s"$clause ")
    }
  }

  def getPrimaryKeyClause(parameters: mutable.Map[String, String]): String = {
    val sb = new StringBuilder()
    sb.append(parameters.get(PARTITION_BY).map(v => {
      val primaryKey = {
        v match {
          case PRIMARY_KEY => ""
          case _ => s"$PRIMARY_KEY ($v, $SHADOW_COLUMN_NAME)"
        }
      }
      primaryKey
    }).getOrElse(s"$PRIMARY_KEY ($SHADOW_COLUMN_NAME)"))
    sb.toString
  }

  def ddlExtensionString(parameters: mutable.Map[String, String],
      isRowTable: Boolean, isShadowTable: Boolean): String = {
    val sb = new StringBuilder()

    if (!isShadowTable) {
      sb.append(parameters.remove(PARTITION_BY).map(v => {
        val (parClause) = {
          v match {
            case PRIMARY_KEY => if (isRowTable){
              PRIMARY_KEY
            }
            else {
              throw new DDLException("Column table cannot be partitioned on" +
                  " PRIMARY KEY as no primary key")
            }
            case _ => s"COLUMN ($v) "
          }
        }
        s"$GEM_PARTITION_BY $parClause "
      }
      ).getOrElse(if (isRowTable) EMPTY_STRING else s"$GEM_PARTITION_BY COLUMN ($SHADOW_COLUMN_NAME) "))
    } else {
      parameters.remove(PARTITION_BY).map(v => {
        v match {
          case PRIMARY_KEY => throw new DDLException("Column table cannot be partitioned on" +
              " PRIMARY KEY as no primary key")
          case _ =>
        }
      })
    }

    if (!isShadowTable) {
      sb.append(parameters.remove(COLOCATE_WITH).map(v => s"$GEM_COLOCATE_WITH ($v) ")
          .getOrElse(EMPTY_STRING))
    }

    parameters.remove(REPLICATE).foreach(v =>
      if (v.toBoolean) sb.append(GEM_REPLICATE).append(' ')
      else if (!parameters.contains(BUCKETS))
        sb.append(GEM_BUCKETS).append(' ').append(
          ExternalStoreUtils.DEFAULT_TABLE_BUCKETS).append(' '))
    sb.append(parameters.remove(BUCKETS).map(v => s"$GEM_BUCKETS $v ")
        .getOrElse(EMPTY_STRING))

    sb.append(parameters.remove(REDUNDANCY).map(v => s"$GEM_REDUNDANCY $v ")
        .getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(RECOVERYDELAY).map(v => s"$GEM_RECOVERYDELAY $v ")
        .getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(MAXPARTSIZE).map(v => s"$GEM_MAXPARTSIZE $v ")
        .getOrElse(EMPTY_STRING))

    // if OVERFLOW has been provided, then use HEAPPERCENT as the default
    // eviction policy (unless overridden explicitly)
    val defaultEviction = if (parameters.contains(OVERFLOW)) GEM_HEAPPERCENT
    else EMPTY_STRING
    if (!isShadowTable) {
      sb.append(parameters.remove(EVICTION_BY).map(v => s"$GEM_EVICTION_BY $v ")
          .getOrElse(defaultEviction))
    } else {
      sb.append(parameters.remove(EVICTION_BY).map(v => {
        v.contains(LRUCOUNT) match {
          case true => throw new DDLException(
            "Column table cannot take LRUCOUNT as Evcition Attributes")
          case _ =>
        }
        s"$GEM_EVICTION_BY $v "
      }).getOrElse(defaultEviction))
    }

    parameters.remove(PERSISTENT).foreach { v =>
      if (v.equalsIgnoreCase("async") || v.equalsIgnoreCase("true")) {
        sb.append(s"$GEM_PERSISTENT ASYNCHRONOUS ")
      } else if (v.equalsIgnoreCase("sync")) {
        sb.append(s"$GEM_PERSISTENT SYNCHRONOUS ")
      } else if (!v.equalsIgnoreCase("false")) {
        sb.append(s"$GEM_PERSISTENT $v ")
      }
    }
    sb.append(parameters.remove(SERVER_GROUPS).map(v => s"$GEM_SERVER_GROUPS $v ")
        .getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(OFFHEAP).map(v => s"$GEM_OFFHEAP $v ")
        .getOrElse(EMPTY_STRING))
    parameters.remove(OVERFLOW).foreach(v => sb.append(s"$GEM_OVERFLOW "))

    sb.append(parameters.remove(EXPIRE).map(v => {
      if (!isRowTable) {
        throw new DDLException("Expiry for Column table is not supported")
      }
      s"$GEM_EXPIRE ENTRY WITH TIMETOLIVE $v ACTION DESTROY"
    }).getOrElse(EMPTY_STRING))

    sb.toString()
  }

  def getPartitioningColumn(parameters: mutable.Map[String, String]) : Seq[String] = {
    parameters.get(PARTITION_BY).map(v => {
      v.split(",").toSeq.map(a => a.trim)
    }).getOrElse(Seq.empty[String])
  }


  def validateConnProps(parameters: mutable.Map[String, String]): Unit ={
    parameters.keys.forall(v => {
      if(!ddlOptions.contains(v.toString.toUpperCase())){
        throw new AnalysisException(s"Unknown options $v specified while creating table ")
      }
      true
    })
  }

  /**
   * Returns a PreparedStatement that inserts a row into table via conn.
   */
  def insertStatement(conn: Connection, table: String, rddSchema: StructType,
      upsert: Boolean): PreparedStatement = {
    val sql = if (!upsert) {
      new StringBuilder(s"INSERT INTO $table (")
    }
    else {
      new StringBuilder(s"PUT INTO $table (")
    }
    var fieldsLeft = rddSchema.fields.length
    rddSchema.fields map { field =>
      sql.append(field.name)
      if (fieldsLeft > 1) sql.append(", ") else sql.append(")")
      fieldsLeft = fieldsLeft - 1
    }
    sql.append("VALUES (")
    fieldsLeft = rddSchema.fields.length
    while (fieldsLeft > 0) {
      sql.append("?")
      if (fieldsLeft > 1) sql.append(", ") else sql.append(")")
      fieldsLeft = fieldsLeft - 1
    }
    conn.prepareStatement(sql.toString())
  }

  /**
   * Saves the RDD to the database in a single transaction.
   */
  def saveTable(
      df: DataFrame,
      url: String,
      table: String,
      properties: Properties = new Properties(),
      upsert: Boolean = false) {
    val dialect = JdbcDialects.get(url)
    val nullTypes: Array[Int] = df.schema.fields.map { field =>
      dialect.getJDBCType(field.dataType).map(_.jdbcNullType).getOrElse(
        field.dataType match {
          case IntegerType => java.sql.Types.INTEGER
          case LongType => java.sql.Types.BIGINT
          case DoubleType => java.sql.Types.DOUBLE
          case FloatType => java.sql.Types.REAL
          case ShortType => java.sql.Types.INTEGER
          case ByteType => java.sql.Types.INTEGER
          case BooleanType => java.sql.Types.BIT
          case StringType => java.sql.Types.CLOB
          case BinaryType => java.sql.Types.BLOB
          case TimestampType => java.sql.Types.TIMESTAMP
          case DateType => java.sql.Types.DATE
          case t: DecimalType => java.sql.Types.DECIMAL
          case _ => throw new IllegalArgumentException(
            s"Can't translate null value for field $field")
        })
    }

    val rddSchema = df.schema
    val driver: String = DriverRegistry.getDriverClassName(url)
    val getConnection: () => Connection = JDBCRDD.getConnector(driver, url, properties)
    val batchSize = properties.getProperty("batchsize", "1000").toInt
    df.foreachPartition { iterator =>
      savePartition(getConnection, table, iterator, rddSchema, nullTypes, batchSize, upsert)
    }
  }

  /**
   * Saves a partition of a DataFrame to the JDBC database.  This is done in
   * a single database transaction in order to avoid repeatedly inserting
   * data as much as possible.
   *
   * It is still theoretically possible for rows in a DataFrame to be
   * inserted into the database more than once if a stage somehow fails after
   * the commit occurs but before the stage can return successfully.
   *
   * This is not a closure inside saveTable() because apparently cosmetic
   * implementation changes elsewhere might easily render such a closure
   * non-Serializable.  Instead, we explicitly close over all variables that
   * are used.
   */
  def savePartition(
      getConnection: () => Connection,
      table: String,
      iterator: Iterator[Row],
      rddSchema: StructType,
      nullTypes: Array[Int],
      batchSize: Int,
      upsert: Boolean): Iterator[Byte] = {
    val conn = getConnection()
    var committed = false
    try {
      val stmt = insertStatement(conn, table, rddSchema, upsert)
      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          val numFields = rddSchema.fields.length
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              rddSchema.fields(i).dataType match {
                case IntegerType => stmt.setInt(i + 1, row.getInt(i))
                case LongType => stmt.setLong(i + 1, row.getLong(i))
                case DoubleType => stmt.setDouble(i + 1, row.getDouble(i))
                case FloatType => stmt.setFloat(i + 1, row.getFloat(i))
                case ShortType => stmt.setInt(i + 1, row.getShort(i))
                case ByteType => stmt.setInt(i + 1, row.getByte(i))
                case BooleanType => stmt.setBoolean(i + 1, row.getBoolean(i))
                case StringType => stmt.setString(i + 1, row.getString(i))
                case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](i))
                case TimestampType => stmt.setTimestamp(i + 1, row.getAs[java.sql.Timestamp](i))
                case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](i))
                case t: DecimalType => stmt.setBigDecimal(i + 1, row.getDecimal(i))
                case _ => throw new IllegalArgumentException(
                  s"Can't translate non-null value for field $i")
              }
            }
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      conn.commit()
      committed = true
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        conn.rollback()
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
    Array[Byte]().iterator
  }
}
