/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
import java.sql.{CallableStatement, Connection, SQLException}

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.Constant
import io.snappydata.impl.SmartConnectorRDDHelper
import org.apache.hadoop.hive.ql.metadata.Table

import org.apache.spark.sql.catalyst.expressions.SortDirection
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.hive.{ExternalTableType, QualifiedTableName, RelationInfo, SnappyStoreHiveCatalog}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.{Logging, Partition}

class SmartConnectorHelper(snappySession: SnappySession) extends Logging {

  private lazy val clusterMode = SnappyContext.getClusterMode(snappySession.sparkContext)

  private var conn: Connection = _
  private var connectionURL: String = _
  private val createSnappyTblString = "call sys.CREATE_SNAPPY_TABLE(?, ?, ?, ?, ?, ?, ?)"
  private val dropSnappyTblString = "call sys.DROP_SNAPPY_TABLE(?, ?, ?)"
  private val createSnappyIdxString = "call sys.CREATE_SNAPPY_INDEX(?, ?, ?, ?)"
  private val dropSnappyIdxString = "call sys.DROP_SNAPPY_INDEX(?, ?)"
  private val getMetaDataStmtString = "call sys.GET_TABLE_METADATA(?, ?, ?, ?, ?, ?, ?, ?)"
  private val createUDFString = "call sys.CREATE_SNAPPY_UDF(?, ?, ?, ?)"
  private val dropUDFString = "call sys.DROP_SNAPPY_UDF(?, ?)"
  private val alterTableStmtString = "call sys.ALTER_SNAPPY_TABLE(?, ?, ?, ?, ?)"
  private var getMetaDataStmt: CallableStatement = _
  private var createSnappyTblStmt: CallableStatement = _
  private var dropSnappyTblStmt: CallableStatement = _
  private var createSnappyIdxStmt: CallableStatement = _
  private var dropSnappyIdxStmt: CallableStatement = _
  private var createUDFStmt: CallableStatement = _
  private var dropUDFStmt: CallableStatement = _
  private var alterTableStmt: CallableStatement = _

  clusterMode match {
    case ThinClientConnectorMode(_, url) =>
      connectionURL = url
      initializeConnection()
    case _ =>
  }

  def initializeConnection(): Unit = {
    val jdbcOptions = new JDBCOptions(connectionURL + getSecurePart + ";route-query=false;", "",
      Map("driver" -> Constant.JDBC_CLIENT_DRIVER))
    conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    createSnappyTblStmt = conn.prepareCall(createSnappyTblString)
    dropSnappyTblStmt = conn.prepareCall(dropSnappyTblString)
    createSnappyIdxStmt = conn.prepareCall(createSnappyIdxString)
    dropSnappyIdxStmt = conn.prepareCall(dropSnappyIdxString)
    getMetaDataStmt = conn.prepareCall(getMetaDataStmtString)
    createUDFStmt = conn.prepareCall(createUDFString)
    dropUDFStmt = conn.prepareCall(dropUDFString)
    alterTableStmt = conn.prepareCall(alterTableStmtString)
  }

  private def getSecurePart: String = {
    var securePart = ""
    val user = snappySession.sqlContext.getConf(Constant.SPARK_STORE_PREFIX + Attribute
        .USERNAME_ATTR, "")
    if (!user.isEmpty) {
      val pass = snappySession.sqlContext.getConf(Constant.SPARK_STORE_PREFIX + Attribute
          .PASSWORD_ATTR, "")
      securePart = s";user=$user;password=$pass"
      logInfo(s"Using $user credentials to securely connect to snappydata cluster")
    }
    securePart
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
    e.getSQLState.startsWith(SQLState.CONNECTIVITY_PREFIX) ||
    e.getSQLState.startsWith(SQLState.LANG_DEAD_STATEMENT)
  }

  def createTable(
      tableIdent: QualifiedTableName,
      provider: String,
      userSpecifiedSchema: Option[StructType],
      schemaDDL: Option[String],
      mode: SaveMode,
      options: Map[String, String],
      isBuiltIn: Boolean): LogicalPlan = {

    snappySession.sessionCatalog.invalidateTable(tableIdent)

    runStmtWithExceptionHandling(executeCreateTableStmt(tableIdent,
      provider, userSpecifiedSchema, schemaDDL, mode, options, isBuiltIn))

    SnappySession.clearAllCache()
    snappySession.sessionCatalog.lookupRelation(tableIdent)
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
    createSnappyTblStmt.setBlob(5, SmartConnectorHelper.getBlob(mode, conn))
    createSnappyTblStmt.setBlob(6, SmartConnectorHelper.getBlob(options, conn))
    createSnappyTblStmt.setBoolean(7, isBuiltIn)
    createSnappyTblStmt.execute()
  }

  def dropTable(tableIdent: QualifiedTableName, ifExists: Boolean,
      isExternal: Boolean): Unit = {
    snappySession.sessionCatalog.invalidateTable(tableIdent)
    runStmtWithExceptionHandling(executeDropTableStmt(tableIdent, ifExists, isExternal))
    SnappyStoreHiveCatalog.registerRelationDestroy()
    SnappySession.clearAllCache()
  }

  def alterTable(tableIdent: QualifiedTableName,
                 isAddColumn: Boolean, column: StructField): Unit = {
    runStmtWithExceptionHandling(executeAlterTableStmt(tableIdent, isAddColumn, column))
    SnappySession.clearAllCache()
  }

  private def executeAlterTableStmt(tableIdent: QualifiedTableName,
                                    isAddColumn: Boolean,
                                    column: StructField): Unit = {
    alterTableStmt.setString(1, tableIdent.table)
    alterTableStmt.setBoolean(2, isAddColumn)
    alterTableStmt.setString(3, column.name)
    alterTableStmt.setString(4, column.dataType.simpleString)
    alterTableStmt.setBoolean(5, column.nullable)
    alterTableStmt.execute()
  }

  private def executeDropTableStmt(tableIdent: QualifiedTableName,
      ifExists: Boolean, isExternal: Boolean): Unit = {
    dropSnappyTblStmt.setString(1, tableIdent.schemaName + "." + tableIdent.table)
    dropSnappyTblStmt.setBoolean(2, ifExists)
    dropSnappyTblStmt.setBoolean(3, isExternal)
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
    createSnappyIdxStmt.setBlob(3, SmartConnectorHelper.getBlob(indexColumns, conn))
    createSnappyIdxStmt.setBlob(4, SmartConnectorHelper.getBlob(options, conn))
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

    val embdClusterRelDestroyVersion = getMetaDataStmt.getInt(7)

    val t: Table = {
      val tableObjectBytes = tableObjectBlob.getBytes(1, tableObjectBlob.length().toInt)
      val baip = new ByteArrayInputStream(tableObjectBytes)
      val ois = new ObjectInputStream(baip)
      ois.readObject().asInstanceOf[Table]
    }

    if (ExternalTableType.isTableBackedByRegion(ExternalTableType.getTableType(t))) {
      val bucketCount = getMetaDataStmt.getInt(3)
      val indexColsString = getMetaDataStmt.getString(5)
      val indexCols = Option(indexColsString) match {
        case Some(str) => str.split(":")
        case None => Array.empty[String]
      }
      val pkColsString = getMetaDataStmt.getString(8)
      val pkCols = Option(pkColsString) match {
        case Some(str) => str.split(":")
        case None => Array.empty[String]
      }

      if (bucketCount > 0) {
        val partitionCols = getMetaDataStmt.getString(4).split(":")
        val bucketToServerMappingStr = getMetaDataStmt.getString(6)
        val allNetUrls = SmartConnectorRDDHelper.setBucketToServerMappingInfo(
          bucketToServerMappingStr, snappySession)
        val partitions = SmartConnectorRDDHelper.getPartitions(allNetUrls)
        (t, RelationInfo(bucketCount, isPartitioned = true, partitionCols.toSeq,
          indexCols, pkCols, partitions, embdClusterRelDestroyVersion))
      } else {
        val replicaToNodesInfo = getMetaDataStmt.getString(6)
        val allNetUrls = SmartConnectorRDDHelper.setReplicasToServerMappingInfo(
          replicaToNodesInfo, snappySession)
        val partitions = SmartConnectorRDDHelper.getPartitions(allNetUrls)
        (t, RelationInfo(1, isPartitioned = false, Nil, indexCols, pkCols,
          partitions, embdClusterRelDestroyVersion))
      }
    } else {
      // external tables (with source as csv, parquet etc.)
      (t, RelationInfo(1, isPartitioned = false, Nil, Array.empty[String],
        Array.empty[String], Array.empty[Partition], embdClusterRelDestroyVersion))
    }
  }

  private def executeMetaDataStatement(tableName: String): Unit = {
    getMetaDataStmt.setString(1, tableName)
    // Hive table object
    getMetaDataStmt.registerOutParameter(2, java.sql.Types.BLOB)
    // bucket count
    getMetaDataStmt.registerOutParameter(3, java.sql.Types.INTEGER)
    // partitioning columns
    getMetaDataStmt.registerOutParameter(4, java.sql.Types.VARCHAR)
    // index columns
    getMetaDataStmt.registerOutParameter(5, java.sql.Types.VARCHAR)
    // bucket to server or replica to server mapping
    getMetaDataStmt.registerOutParameter(6, java.sql.Types.CLOB)
    // relation destroy version
    getMetaDataStmt.registerOutParameter(7, java.sql.Types.INTEGER)
    // primary key columns
    getMetaDataStmt.registerOutParameter(8, java.sql.Types.VARCHAR)
    getMetaDataStmt.execute
  }

  def executeCreateUDFStatement(db: String, functionName: String,
      className: String, jarURI: String): Unit = {
    createUDFStmt.setString(1, db)
    createUDFStmt.setString(2, functionName)
    createUDFStmt.setString(3, className)
    createUDFStmt.setString(4, jarURI)
    createUDFStmt.execute
  }

  def executeDropUDFStatement(db: String, functionName: String): Unit = {
    dropUDFStmt.setString(1, db)
    dropUDFStmt.setString(2, functionName)
    dropUDFStmt.execute
  }
}

object SmartConnectorHelper {

  def getBlob(value: Any, conn: Connection): java.sql.Blob = {
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
