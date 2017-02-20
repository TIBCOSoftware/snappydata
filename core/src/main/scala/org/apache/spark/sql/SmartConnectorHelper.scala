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
package org.apache.spark.sql

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.sql.{SQLException, CallableStatement, Connection}
import java.util.Properties

import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.Property
import io.snappydata.impl.SparkShellRDDHelper
import org.apache.hadoop.hive.metastore.api.Table

import org.apache.spark.sql.catalyst.expressions.SortDirection
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.hive.{ExternalTableType, RelationInfo, QualifiedTableName, SnappyStoreHiveCatalog}
import org.apache.spark.sql.sources.JdbcExtendedUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, Logging, SparkContext}

object SmartConnectorHelper extends Logging {
  private lazy val session = SnappyContext(null: SparkContext).snappySession
  private lazy val clusterMode = SnappyContext.getClusterMode(session.sparkContext)

  private lazy val connFactory = {
    clusterMode match {
      case ThinClientConnectorMode(_, props) =>
        JdbcUtils.createConnectionFactory(
          Property.ClusterURL.getOption(session.sparkContext.conf).get +
              ";route-query=false;" + props, new Properties())
      case _ =>
        throw new AnalysisException("Not expected to be called for " + clusterMode)
    }
  }

  private var conn: Connection = _
  private val createSnappyTblString = "call sys.CREATE_SNAPPY_TABLE(?, ?, ?, ?, ?, ?, ?)"
  private val dropSnappyTblString = "call sys.DROP_SNAPPY_TABLE(?, ?)"
  private val createSnappyIdxString = "call sys.CREATE_SNAPPY_INDEX(?, ?, ?, ?)"
  private val dropSnappyIdxString = "call sys.DROP_SNAPPY_INDEX(?, ?)"
  private val getMetaDataStmtString = "call sys.GET_TABLE_METADATA(?, ?, ?, ?, ?, ?)"
  private var getMetaDataStmt: CallableStatement = _
  private var createSnappyTblStmt: CallableStatement = _
  private var dropSnappyTblStmt: CallableStatement = _
  private var createSnappyIdxStmt: CallableStatement = _
  private var dropSnappyIdxStmt: CallableStatement = _

  clusterMode match {
    case ThinClientConnectorMode(_, props) =>
      initializeConnection()
    case _ =>
  }

  private def initializeConnection(): Unit = {
    conn = connFactory()
    createSnappyTblStmt =  conn.prepareCall(createSnappyTblString)
    dropSnappyTblStmt = conn.prepareCall(dropSnappyTblString)
    createSnappyIdxStmt = conn.prepareCall(createSnappyIdxString)
    dropSnappyIdxStmt = conn.prepareCall(dropSnappyIdxString)
    getMetaDataStmt  = conn.prepareCall(getMetaDataStmtString)
  }

  private def runStmtWithExceptionHandling[T](function: => T): T = {
    try {
      function
    } catch {
      case e: SQLException if isConnectionException(e) =>
        // attempt to create a new connection if connection
        // is closed
        conn.close()
        initializeConnection()
        function
    }
  }

  private def isConnectionException(e: SQLException): Boolean = {
    e.getSQLState.startsWith(SQLState.CONNECTIVITY_PREFIX)
  }

  def createTable(
      tableIdent: QualifiedTableName,
      provider: String,
      userSpecifiedSchema: Option[StructType],
      schemaDDL: Option[String],
      mode: SaveMode,
      options: Map[String, String],
      isBuiltIn: Boolean): LogicalPlan = {

    runStmtWithExceptionHandling(executeCreateTableStmt(tableIdent,
      provider, userSpecifiedSchema, schemaDDL, mode, options, isBuiltIn))

    session.sessionCatalog.lookupRelation(tableIdent)
  }

  private def executeCreateTableStmt(tableIdent: QualifiedTableName,
      provider: String, userSpecifiedSchema: Option[StructType], schemaDDL: Option[String],
      mode: SaveMode, options: Map[String, String], isBuiltIn: Boolean ): Unit = {
    createSnappyTblStmt.setString(1, tableIdent.schemaName + "." + tableIdent.table)
    createSnappyTblStmt.setString(2, provider)
    val jsonSchema = userSpecifiedSchema match {
      case Some(uSchema) => uSchema.json
      case None => null
    }
    createSnappyTblStmt.setString(3, jsonSchema)
    createSnappyTblStmt.setString(4, schemaDDL.orNull)
    createSnappyTblStmt.setBlob(5, getBlob(mode))
    createSnappyTblStmt.setBlob(6, getBlob(options))
    createSnappyTblStmt.setBoolean(7, isBuiltIn)
    createSnappyTblStmt.execute()
  }

  def createTable(
      tableIdent: QualifiedTableName,
      provider: String,
      userSpecifiedSchema: Option[StructType],
      schemaDDL: Option[String],
      partitionColumns: Array[String],
      mode: SaveMode,
      options: Map[String, String],
      query: LogicalPlan,
      isBuiltIn: Boolean): LogicalPlan = {
    throw new AnalysisException("Not yet implemented")
  }

  def dropTable(tableIdent: QualifiedTableName, ifExists: Boolean = false): Unit = {
    session.sessionCatalog.invalidateTable(tableIdent)
    runStmtWithExceptionHandling(executeDropTableStmt(tableIdent, ifExists))
    SnappyStoreHiveCatalog.registerRelationDestroy()
  }

  private def executeDropTableStmt(tableIdent: QualifiedTableName,
      ifExists: Boolean): Unit = {
    dropSnappyTblStmt.setString(1, tableIdent.schemaName + "." + tableIdent.table)
    dropSnappyTblStmt.setBoolean(2, ifExists)
    dropSnappyTblStmt.execute()
  }

  def createIndex(indexIdent: QualifiedTableName,
      tableIdent: QualifiedTableName,
      indexColumns: Map[String, Option[SortDirection]],
      options: Map[String, String]): Unit = {
    runStmtWithExceptionHandling(
      executeCreateIndexStmt(indexIdent, tableIdent, indexColumns, options))
    SnappySession.clearAllCache()
  }

  private def executeCreateIndexStmt(indexIdent: QualifiedTableName,
      tableIdent: QualifiedTableName,
      indexColumns: Map[String, Option[SortDirection]],
      options: Map[String, String]): Unit = {
    createSnappyIdxStmt.setString(1, indexIdent.schemaName + "." + indexIdent.table)
    createSnappyIdxStmt.setString(2, tableIdent.schemaName + "." + tableIdent.table)
    createSnappyIdxStmt.setBlob(3, getBlob(indexColumns))
    createSnappyIdxStmt.setBlob(4, getBlob(options))
    createSnappyIdxStmt.execute()
  }

  def dropIndex(indexName: QualifiedTableName, ifExists: Boolean): Unit = {
    runStmtWithExceptionHandling(executeDropIndexStmt(indexName, ifExists))
    SnappyStoreHiveCatalog.registerRelationDestroy()
    SnappySession.clearAllCache()
  }

  private def executeDropIndexStmt(indexIdent: QualifiedTableName, ifExists: Boolean): Unit = {
    dropSnappyIdxStmt.setString(1, indexIdent.schemaName + "." + indexIdent.table)
    dropSnappyIdxStmt.setBoolean(2, ifExists)
    dropSnappyIdxStmt.execute()
  }

  def getHiveTableAndMetadata(in: QualifiedTableName): (Table, RelationInfo) = {

    runStmtWithExceptionHandling(executeMetaDataStatement(in.toString))

    val tableObjectBlob = Option(getMetaDataStmt.getBlob(2)).
        getOrElse(throw new TableNotFoundException(s"Table $in not found"))

    val t: Table = {
      val tableObjectBytes = tableObjectBlob.getBytes(1, tableObjectBlob.length().toInt)
      val baip = new ByteArrayInputStream(tableObjectBytes)
      val ois = new ObjectInputStream(baip)
      ois.readObject().asInstanceOf[Table]
    }

    if (ExternalTableType.isTableBackedByRegion(t)) {
      val bucketCount = getMetaDataStmt.getInt(3)
      val indexColsString = getMetaDataStmt.getString(5)
      val indexCols = Option(indexColsString) match {
        case Some(str) => str.split(":")
        case None => Array.empty[String]
      }
      if (bucketCount > 0) {
        val partitionCols = getMetaDataStmt.getString(4).split(":")
        val bucketToServerMappingStr = getMetaDataStmt.getString(6)
        val allNetUrls = SparkShellRDDHelper.setBucketToServerMappingInfo(bucketToServerMappingStr)
        val partitions = SparkShellRDDHelper.getPartitions(allNetUrls)
        (t, new RelationInfo(bucketCount, partitionCols.toSeq, indexCols, partitions))
      } else {
        val replicaToNodesInfo = getMetaDataStmt.getString(6)
        val allNetUrls = SparkShellRDDHelper.setReplicasToServerMappingInfo(replicaToNodesInfo)
        val partitions = SparkShellRDDHelper.getPartitions(allNetUrls)
        (t, new RelationInfo(1, Seq.empty[String], indexCols, partitions))
      }
    } else {
      // external tables (with source as csv, parquet etc.)
      (t, new RelationInfo(1, Seq.empty[String], Array.empty[String], Array.empty[Partition]))
    }
  }

  def executeMetaDataStatement(tableName: String): Unit = {
    getMetaDataStmt.setString(1, tableName)
    getMetaDataStmt.registerOutParameter(2, java.sql.Types.BLOB) /*Hive table object*/
    getMetaDataStmt.registerOutParameter(3, java.sql.Types.INTEGER) /*bucket count*/
    getMetaDataStmt.registerOutParameter(4, java.sql.Types.VARCHAR) /*partitioning columns*/
    getMetaDataStmt.registerOutParameter(5, java.sql.Types.VARCHAR) /*index columns*/
    getMetaDataStmt.registerOutParameter(6, java.sql.Types.CLOB) /*bucket to server or replica to server mapping*/
    getMetaDataStmt.execute
  }

  def getBlob(value: Any, conn: Connection = conn): java.sql.Blob = {
    val serializedValue: Array[Byte] = serialize(value)
    val blob = conn.createBlob()
    blob.setBytes(1, serializedValue)
    blob
  }

  def serialize(value: Any): Array[Byte] = {
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream()
    val os: ObjectOutputStream = new ObjectOutputStream(baos)
    os.writeObject(value)
    os.close()
    baos.toByteArray
  }

  def deserialize(value: Array[Byte]): Any = {
    val baip = new ByteArrayInputStream(value)
    val ois = new ObjectInputStream(baip)
    ois.readObject()
  }

}