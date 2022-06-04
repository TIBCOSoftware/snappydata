/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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
package io.snappydata.gemxd

import java.io.{File, InputStream}
import java.util.{Iterator => JIterator}
import java.{lang, util}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.shared.Version
import com.gemstone.gemfire.internal.{ByteArrayDataInput, ClassPathLoader, GemFireVersion}
import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor
import com.pivotal.gemfirexd.internal.impl.sql.execute.ValueRow
import com.pivotal.gemfirexd.internal.snappy._
import io.snappydata.cluster.ExecutorInitiator
import io.snappydata.impl.LeadImpl
import io.snappydata.recovery.RecoveryService
import io.snappydata.remote.interpreter.SnappyInterpreterExecute
import io.snappydata.{ServiceManager, SnappyEmbeddedTableStatsProviderService}

import org.apache.spark.Logging
import org.apache.spark.scheduler.cluster.SnappyClusterManager
import org.apache.spark.serializer.{KryoSerializerPool, StructTypeSerializer}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.collection.ToolsCallbackInit
import org.apache.spark.sql.{SaveMode, SnappyContext}

/**
 * Callbacks that are sent by GemXD to Snappy for cluster management
 */
object ClusterCallbacksImpl extends ClusterCallbacks with Logging {

  CallbackFactoryProvider.setClusterCallbacks(this)

  private[snappydata] def initialize(): Unit = {
    // nothing to be done; singleton constructor does all
  }

  override def getLeaderGroup: java.util.HashSet[String] = {
    val leaderServerGroup = new java.util.HashSet[String]
    leaderServerGroup.add(LeadImpl.LEADER_SERVERGROUP)
    leaderServerGroup
  }

  override def launchExecutor(driverUrl: String,
      driverDM: InternalDistributedMember): Unit = {
    val url = if (driverUrl == null || driverUrl == "") {
      logInfo(s"call to launchExecutor but driverUrl is invalid. $driverUrl")
      None
    }
    else {
      Some(driverUrl)
    }
    logInfo(s"invoking startOrTransmute with $url")
    ExecutorInitiator.startOrTransmuteExecutor(url, driverDM)
  }

  override def getDriverURL: String = {
    SnappyClusterManager.cm.map(_.schedulerBackend) match {
      case Some(backend) if backend ne null =>
        val driverUrl = backend.driverUrl
        if ((driverUrl ne null) && !driverUrl.isEmpty) {
          logInfo(s"returning driverUrl=$driverUrl")
        }
        driverUrl
      case _ => null
    }
  }

  override def stopExecutor(): Unit = {
    ExecutorInitiator.stop()
  }

  override def getSQLExecute(df: AnyRef, sql: String, schema: String, ctx: LeadNodeExecutionContext,
      v: Version, isPreparedStatement: Boolean, isPreparedPhase: Boolean,
      pvs: ParameterValueSet, pvsTypes: Array[Int]): SparkSQLExecute = {
    if (isPreparedStatement && isPreparedPhase) {
      new SparkSQLPrepareImpl(sql, schema, ctx, v)
    } else {
      new SparkSQLExecuteImpl(df, sql, schema, ctx, v, Option(pvs), pvsTypes)
    }
  }

  override  def getSampleInsertExecute(baseTable: String, ctx: LeadNodeExecutionContext,
    v: Version, dvdRows: util.List[Array[DataValueDescriptor]],
    serializedDVDs: Array[Byte]): SparkSQLExecute = {
     new SparkSampleInsertExecuteImpl(baseTable, dvdRows, serializedDVDs, ctx, v)
  }

  override def readDataType(in: ByteArrayDataInput): AnyRef = {
    // read the DataType
    KryoSerializerPool.deserialize(in.array(), in.position(), in.available(), (kryo, input) => {
      val result = StructTypeSerializer.readType(kryo, input)
      // move the cursor to the new position
      in.setPosition(input.position())
      result
    })
  }

  override def getRowIterator(dvds: Array[DataValueDescriptor],
      types: Array[Int], precisions: Array[Int], scales: Array[Int],
      dataTypes: Array[AnyRef], in: ByteArrayDataInput): JIterator[ValueRow] = {
    SparkSQLExecuteImpl.getRowIterator(dvds, types, precisions, scales,
      dataTypes, in)
  }

  override def clearSnappySessionForConnection(
      connectionId: java.lang.Long): Unit = {
    SnappySessionPerConnection.removeSnappySession(connectionId)
  }

  override def publishColumnTableStats(): Unit = {
    SnappyEmbeddedTableStatsProviderService.publishColumnTableRowCountStats()
  }

  override def getClusterType: String = {
    GemFireCacheImpl.setGFXDSystem(true)
    // AQP version if available
    val is: InputStream = ClassPathLoader.getLatest.getResourceAsStream(
      classOf[SnappyDataVersion], SnappyDataVersion.AQP_VERSION_PROPERTIES)
    if (is ne null) try {
      GemFireVersion.getInstance(classOf[SnappyDataVersion], SnappyDataVersion
          .AQP_VERSION_PROPERTIES)
    } finally {
      is.close()
    }
    GemFireVersion.getClusterType
  }

  override def exportData(connId: lang.Long, exportUri: String,
      formatType: String, tableNames: String, ignoreError: lang.Boolean): Unit = {
    val session = SnappySessionPerConnection.getSnappySessionForConnection(connId)
    if (Misc.isSecurityEnabled) {
      session.conf.set(Attribute.USERNAME_ATTR,
        Misc.getMemStore.getBootProperty(Attribute.USERNAME_ATTR))
      session.conf.set(Attribute.PASSWORD_ATTR,
        Misc.getMemStore.getBootProperty(Attribute.PASSWORD_ATTR))
    }

    var tablesArr = if (tableNames.equalsIgnoreCase("all")) {
      RecoveryService.getTables.map(ct =>
        ct.storage.locationUri match {
          case Some(_) => null // external tables will not be exported
          case None =>
            ct.identifier.database match {
              case Some(db) => db + "." + ct.identifier.table
              case None => ct.identifier.table
            }
        }
      ).filter(_ != null)
    } else tableNames.split(",").map(_.trim).toSeq

    logDebug(s"Using connection ID: $connId\n Export path:" +
        s" $exportUri\n Format Type: $formatType\n Table names: $tableNames")

    val exportPath = if (exportUri.endsWith(File.separator)) {
      exportUri.substring(0, exportUri.length - 1) +
          s"_${System.currentTimeMillis()}" + File.separator
    } else {
      exportUri + s"_${System.currentTimeMillis()}" + File.separator
    }
    var failedTables = Seq.empty[String]
    tablesArr.foreach(f = table => {
      Try {
        val tableData = session.sql(s"select * from $table;")
        val savePath = exportPath + File.separator + table.toUpperCase
        tableData.write.mode(SaveMode.Overwrite).option("header", "true").format(formatType)
            .save(savePath)
        logDebug(s"EXPORT_DATA procedure exported table $table in $formatType format" +
            s"at path $savePath ")
      } match {
        case scala.util.Success(_) =>
        case scala.util.Failure(exception) =>
          logError(s"Error recovering table: $table.")
          tablesArr = tablesArr.filter(_!=table)
          failedTables = failedTables :+ table
          if (!ignoreError) {
            throw new Exception(exception)
          }
      }
    })
    logInfo(
      s"""Successfully exported ${tablesArr.size} tables.
         |Exported tables are: ${tablesArr.mkString(", ")}
         |Failed to export ${failedTables.size} tables.
         |Failed tables are ${failedTables.mkString(", ")}""".stripMargin)
    generateLoadScripts(connId, exportPath, formatType, tablesArr)
  }

  override def exportDDLs(connId: lang.Long, exportUri: String): Unit = {
    val session = SnappySessionPerConnection.getSnappySessionForConnection(connId)
    val filePath = if (exportUri.endsWith(File.separator)) {
      exportUri.substring(0, exportUri.length - 1) +
          s"_${System.currentTimeMillis()}" + File.separator
    } else {
      exportUri + s"_${System.currentTimeMillis()}" + File.separator
    }
    val arrBuf: ArrayBuffer[String] = ArrayBuffer.empty

    RecoveryService.getAllDDLs.foreach(ddl => {
      arrBuf.append(ddl.trim + ";\n")
    })
    session.sparkContext.parallelize(arrBuf, 1).saveAsTextFile(filePath)
    logInfo(s"Successfully exported ${arrBuf.size} DDL statements.")
  }

  /**
   * generates spark-shell code which helps user to reload
   * data exported through EXPORT_DATA procedure
   */
  def generateLoadScripts(connId: lang.Long, exportPath: String,
      formatType: String, tablesArr: Seq[String]): Unit = {
    val session = SnappySessionPerConnection.getSnappySessionForConnection(connId)
    var loadScriptString = ""

    val generatedScriptPath = s"${exportPath.replaceAll("/$", "")}_load_scripts"
    tablesArr.foreach(table => {
      val tableExternal = s"temp_${table.replace('.', '_')}"
      val additionalOptions = formatType match {
        case "csv" => ",header 'true'"
        case _ => ""
      }
      // todo do testing for all formats and ensure generated scripts handles all scenarios
      loadScriptString +=
          s"""
             |CREATE EXTERNAL TABLE $tableExternal USING ${formatType}
             |OPTIONS (PATH '${exportPath}/${table.toUpperCase}'${additionalOptions});
             |INSERT OVERWRITE $table SELECT * FROM $tableExternal;
             |
        """.stripMargin
    })
    session.sparkContext.parallelize(Seq(loadScriptString), 1).saveAsTextFile(generatedScriptPath)
  }

  override def setLeadClassLoader(): Unit = {
    val instance = ServiceManager.currentFabricServiceInstance
    instance match {
      case li: LeadImpl =>
        val loader = li.urlclassloader
        if (loader != null) {
          Thread.currentThread().setContextClassLoader(loader)
        }
      case _ =>
    }
  }

  override def getInterpreterExecution(sql: String, v: Version,
    connId: lang.Long): InterpreterExecute = new SnappyInterpreterExecute(sql, connId)

  override def isUserAuthorizedForExternalTable (user: String, table: String): Boolean = {
    val tcb = ToolsCallbackInit.toolsCallback
    if (tcb.isUserAuthorizedForExtTable(user, Some(TableIdentifier(table))) != null) return false
    true
  }

  override def cancelJobGroup(groupId: String): Unit = {
    SnappyContext.globalSparkContext.cancelJobGroup(groupId)
  }
}
