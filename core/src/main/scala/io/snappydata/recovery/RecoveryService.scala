/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package io.snappydata.recovery

import java.net.URI
import java.util.function.BiConsumer
import com.pivotal.gemfirexd.internal.engine.ui.{SnappyExternalTableStats, SnappyIndexStats, SnappyRegionStats}
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper
import com.gemstone.gemfire.internal.shared.SystemProperties
import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.ddl.{DDLConflatable, GfxdDDLQueueEntry, GfxdDDLRegionQueue}
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector.ListResultCollectorValue
import com.pivotal.gemfirexd.internal.engine.distributed.message.PersistentStateInRecoveryMode
import com.pivotal.gemfirexd.internal.engine.distributed.message.PersistentStateInRecoveryMode.RecoveryModePersistentView
import com.pivotal.gemfirexd.internal.engine.sql.execute.RecoveredMetadataRequestMessage
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.impl.jdbc.Util
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.Constant

import org.apache.spark.sql.{SnappyContext, SnappyParser, SnappySession}
import io.snappydata.sql.catalog.ConnectorExternalCatalog
import io.snappydata.thrift.{CatalogFunctionObject, CatalogMetadataDetails, CatalogSchemaObject, CatalogTableObject}

import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogFunction, CatalogTable}
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructField, StructType}
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.hive.{HiveClientUtil, SnappyHiveExternalCatalog}

object RecoveryService extends Serializable with Logging {

  private def isGrantRevokeStatement(conflatable: DDLConflatable) = {
    val sqlText = conflatable.getValueToConflate
    // return (sqlText != null && GRANTREVOKE_PATTERN.matcher(sqlText).matches());
    sqlText != null && (sqlText.toUpperCase.startsWith("GRANT")
        || sqlText.toUpperCase.startsWith("REVOKE"))
  }

  /* This should dump the real ddls into the o/p file */
  def getAllDDLs(): mutable.Buffer[String] = {
    val ddlBuffer: mutable.Buffer[String] = List.empty.toBuffer
    if (!Misc.getGemFireCache.isSnappyRecoveryMode) {
      import scala.collection.JavaConversions._

      val dd = Misc.getMemStore.getDatabase.getDataDictionary
      if (dd == null) {
        throw Util.generateCsSQLException(SQLState.SHUTDOWN_DATABASE, Attribute.GFXD_DBNAME)
      }
      dd.lockForReadingRT(null)
      val ddlQ = new GfxdDDLRegionQueue(Misc.getMemStore.getDDLStmtQueue.getRegion)
      ddlQ.initializeQueue(dd)
      val allDDLs: java.util.List[GfxdDDLQueueEntry] = ddlQ.peekAndRemoveFromQueue(-1, -1)
      val preprocessedqueue = ddlQ
          .getPreprocessedDDLQueue(allDDLs, null, null, null, false).iterator;

      for (entry <- preprocessedqueue) {
        logInfo(s"""-------------------------------
            \n RecoveryService:preprocessedQueue:entry ${entry.getValue.toString} =
            => ${entry.getValue.asInstanceOf[DDLConflatable].getValueToConflate}\n
            -----------------------------""")

        // todo add to ddlbuffer
        val qEntry = entry
        val qVal = qEntry.getValue
        if (qVal.isInstanceOf[DDLConflatable]) {
          val conflatable = qVal.asInstanceOf[DDLConflatable]
          val schema = conflatable.getSchemaForTableNoThrow
          if (conflatable.isCreateDiskStore) {
            ddlBuffer.add(conflatable.getValueToConflate)
          } else if (Misc.SNAPPY_HIVE_METASTORE == schema
              || Misc.SNAPPY_HIVE_METASTORE == conflatable.getCurrentSchema
              || Misc.SNAPPY_HIVE_METASTORE == conflatable.getRegionToConflate) {
          } else if (conflatable.isAlterTable || conflatable.isCreateIndex ||
              isGrantRevokeStatement(conflatable) ||
              conflatable.isCreateTable || conflatable.isDropStatement ||
              conflatable.isCreateSchemaText) {
            val ddl = conflatable.getValueToConflate
            val ddlLowerCase = ddl.toLowerCase()
            if (!"create[ ]+diskstore".r.findFirstIn(ddlLowerCase).isEmpty ||
                !"create[ ]+index".r.findFirstIn(ddlLowerCase).isEmpty ||
                ddlLowerCase.trim.contains("^grant") ||
                ddlLowerCase.trim.contains("^revoke")) {
              ddlBuffer.add(ddl)
            }
          }
        }
      }
    } else {
      val otherDdls = mostRecentMemberObject.getOtherDDLs.asScala

      otherDdls.foreach(ddl => {
        // todo add grant revoke above.. in if
        val ddlLowerCase = ddl.toLowerCase()
        if (!"create[ ]+diskstore".r.findFirstIn(ddlLowerCase).isEmpty ||
            !"create[ ]+index".r.findFirstIn(ddlLowerCase).isEmpty ||
            ddlLowerCase.trim.contains("^grant") ||
            ddlLowerCase.trim.contains("^revoke")) {
          ddlBuffer.append(ddl)
        }
      })
    }

    val snappyContext = SnappyContext()
    val snappySession = snappyContext.snappySession
    val snappyHiveExternalCatalog = HiveClientUtil
        .getOrCreateExternalCatalog(snappyContext.sparkContext, snappyContext.sparkContext.getConf)
    val dbList = snappyHiveExternalCatalog.listDatabases("*").filter(dbName =>
      !(dbName.equalsIgnoreCase("SYS") || dbName.equalsIgnoreCase("DEFAULT")))
    val allFunctions = dbList.flatMap(dbName => snappyHiveExternalCatalog.listFunctions(dbName, "*")
        .map(func => snappyHiveExternalCatalog.getFunction(dbName, func)))
    val allDatabases = dbList.map(snappyHiveExternalCatalog.getDatabase)
    allFunctions.foreach(func => {
      val funcClass = func.className.split("_")(0)
      val funcRetType = func.className.split("_")(1)
      val funcDdl = s"CREATE FUNCTION ${func.identifier} RETURNS ${funcRetType} " +
          s"AS ${funcClass} USING JAR ${func.resources.map(_.uri)}"
      ddlBuffer.append(funcDdl)
    })
    allDatabases.foreach(db => {
      db.properties.get("orgSqlText") match {
        case Some(str) => if (!str.isEmpty) ddlBuffer.append(str)
        case None => ""
      }
    })

    val tempViewBuffer: mutable.Buffer[String] = List.empty.toBuffer
    val tempTableBuffer: mutable.Buffer[(String, String)] = List.empty.toBuffer

    val allTables = snappyHiveExternalCatalog.getAllTables()
    allTables.foreach(table => {
      // covers create, alter, create view statements
      val allkeys = table.properties.keys
          .filter(f => f.contains("orgSqlText") || f.contains("altTxt"))
          .toSeq.sortBy(_.split("_")(1).toLong)

      for (key <- allkeys) {
        if (key.contains("orgSqlText")) {
          tempTableBuffer append ((key, table.properties(key)))
        } else if (key.contains("alt")) {
          tempTableBuffer append ((key, table.properties(key)))
        }
      }
      table.viewOriginalText match {
        case Some(ddl) => tempViewBuffer.append(s"create view ${table.identifier} as $ddl")
        case None => ""
      }
    })

    ddlBuffer.appendAll(tempViewBuffer)
    val sortedTempTableBuffer = tempTableBuffer.sortBy(tup => tup._1.split("_")(1).toLong).map(_._2)
    ddlBuffer.appendAll(sortedTempTableBuffer)

    val biConsumer = new BiConsumer[String, String] {
      def accept(alias: String, cmd: String) = {
        val cmdFields = cmd.split('|')
        if (cmdFields.length > 1) {
          val repos = cmdFields(1)
          val path = cmdFields(2)
          if (!repos.isEmpty) {
            ddlBuffer.append(s"DEPLOY PACKAGE ${alias} '${cmdFields(0)}' repos '${repos}'")
          } else {
            ddlBuffer.append(s"DEPLOY PACKAGE ${alias} '${cmdFields(0)}' path '${path}'")
          }
        } else {
          ddlBuffer.append(s"DEPLOY JAR ${alias} '${cmdFields(0)}'")
        }
      }
    }
    Misc.getMemStore.getGlobalCmdRgn.forEach(biConsumer)

    logInfo(s"buffer ready.\n $ddlBuffer")
    if (Misc.getGemFireCache.isSnappyRecoveryMode) {
      logInfo(s"RecoveryService - other ddls = " +
          s" ${mostRecentMemberObject.getOtherDDLs.asScala}")
    }
    ddlBuffer
  }

  /* fqtn and bucket number for PR r Column table, -1 indicates replicated row table */
  def getExecutorHost(fqtn: String, bucketId: Int = -1): Seq[String] = {
    // TODO need checking for row replicated/row partitioned/col table
    // Expecting table in the format fqtn i.e. schemaname.tablename
    val schemaName = fqtn.split('.')(0)
    val tableName = fqtn.split('.')(1)
    val (numBuckets, isReplicated) = getNumBuckets(schemaName, tableName)

    val tablePath = '/' + fqtn.replace(".", "/")
    var bucketPath = tablePath
    if (!isReplicated) {
      // bucketPath = PartitionedRegionHelper.getBucketFullPath(tablePath, bucketId)
      bucketPath = s"/${PartitionedRegionHelper.PR_ROOT_REGION_NAME}/" +
          s"${PartitionedRegionHelper.getBucketName(tablePath, bucketId)}"
      // TODO remove replace used and handle it in a proper way
      // bucketPath = bucketPath.replace("/__PR/_B_", "/__PR/_B__")
    }
    // check if the path exists else check path of column buffer.
    // also there could be no data in any.
    // check only row, only col, no data
    if(regionViewSortedSet.contains(bucketPath)) {
      Seq(regionViewSortedSet(bucketPath).lastKey.getExecutorHost)
    } else {
      for (entry <- regionViewSortedSet) {
        logInfo(s"regionViewSortedSet entry: [${entry._1}, ${entry._2}]")
      }
      logWarning(s"regionViewSortedSet doesn't contain bucket path: ${bucketPath}")
      // Seq("localhost")
      null
    }
  }

  /* Table type, PR or replicated, DStore name, numBuckets */
  def getTableDiskInfo(fqtn: String):
  Tuple4[String, Boolean, String, Int] = {
    val parts = fqtn.split("\\.")
    val schema = parts(0)
    val table = parts(1)
    val cObject = mostRecentMemberObject.getCatalogObjects.toArray()
    val cObjArr: Array[AnyRef] = cObject.filter {
      case a: CatalogTableObject => {
        if (a.schemaName == schema && a.tableName == table) {
          true
        } else {
          false
        }
      }
      case _ => false
    }
    val cObj = cObjArr.head.asInstanceOf[CatalogTableObject]
    val tablePath = fqtn.replace(".", "/")
    val numBuckets = mostRecentMemberObject.getPrToNumBuckets.get(tablePath)
    val robj = mostRecentMemberObject.getAllRegionViews.asScala.find(r => {
      val regionPath = r.getRegionPath
      if (PartitionedRegionHelper.isBucketRegion(regionPath)) {
        val pr = PartitionedRegionHelper.getPRPath(regionPath)
        tablePath == pr
      } else {
        tablePath == regionPath
      }
    })
    logInfo(s"is region null? ${robj == null} numbuckets = ${numBuckets}")
    // robj.get.getDiskStoreName
    (cObj.provider, numBuckets != null,
        null,
        numBuckets)
  }

  val regionViewSortedSet: mutable.Map[String,
      mutable.SortedSet[RecoveryModePersistentView]] = mutable.Map.empty

  val persistentObjectMemberMap: mutable.Map[
      InternalDistributedMember, PersistentStateInRecoveryMode] = mutable.Map.empty

  var mostRecentMemberObject: PersistentStateInRecoveryMode = null;
  var memberObject: PersistentStateInRecoveryMode = null;
  var schemaStructMap: mutable.Map[String, StructType] = mutable.Map.empty
  private val versionMap: mutable.Map[String, Int] = collection.mutable.Map.empty
  private val tableColumnIds: mutable.Map[String, Array[Int]] = mutable.Map.empty

  def getTableColumnIds(): mutable.Map[String, Array[Int]] = {
    this.tableColumnIds
  }

  def getVersionMap(): mutable.Map[String, Int] = {
    this.versionMap
  }

  def getSchemaStructMap(): mutable.Map[String, StructType] = {
    this.schemaStructMap
  }


  def createSchemasMap(snappyHiveExternalCatalog: SnappyHiveExternalCatalog): Unit = {
    val snappySession = new SnappySession(SnappyContext().sparkContext)
    val colParser = new SnappyParser(snappySession)
    var schemaString = ""

    snappyHiveExternalCatalog.getAllTables().foreach(table => {
      if (!table.tableType.name.equalsIgnoreCase("view")) {
        // Create statements
        var versionCnt = 1

        // todo: filter view and external tables - we don't have to export data for them?!?

        // -----------
        table.properties.get("schemaJson") match {
          case Some(schemaJsonStr) => {
            val fqtn = table.identifier.database match {
              case Some(schName) => schName + "_" + table.identifier.table
              case None =>
                throw new Exception(s"Schema name not found " +
                    s"for the table ${table.identifier.table}")
            }
            var schema: StructType = DataType.fromJson(schemaJsonStr).asInstanceOf[StructType]

            schemaStructMap.put(s"${versionCnt}#${fqtn}", schema)
            tableColumnIds.put(s"$versionCnt#$fqtn", Range(0, schema.fields.length).toArray)
            versionMap.put(fqtn, versionCnt)
            versionCnt += 1
            // Alter statements

            val altStmtKeys = table.properties.keys
                .filter(_.contains(s"altTxt_")).toSeq
                .sortBy(_.split("_")(1).toLong)
            altStmtKeys.foreach(k => {
              val stmt = table.properties(k).toUpperCase()
              var alteredSchema: StructType = null
              if (stmt.contains(" ADD COLUMN ")) {
                // TODO replace ; at the end also add regex match instead of contains

                val columnString = stmt.substring(stmt.indexOf("ADD COLUMN ") + 11)
                val colNameAndType = (columnString.split("[ ]+")(0), columnString.split("[ ]+")(1))
                val colString = if (columnString.toLowerCase().contains("not null")) {
                  (s"(${colNameAndType._1} ${colNameAndType._2}) not null")
                } else {
                  s"(${colNameAndType._1} ${colNameAndType._2})"
                }
                val field = colParser.parseSQLOnly(colString, colParser.tableSchemaOpt.run())
                match {
                  case Some(fieldSeq) => {
                    val field = fieldSeq.head
                    val builder = new MetadataBuilder
                    builder.withMetadata(field.metadata)
                        .putString("originalSqlType", colNameAndType._2.trim.toLowerCase())
                    StructField(field.name, field.dataType, field.nullable, builder.build())
                  }
                  case None => throw
                      new IllegalArgumentException("alter statement contains no parsable field")
                }
                alteredSchema = new StructType((schema ++ new StructType(Array(field))).toArray)

              } else if (stmt.contains("DROP COLUMN ")) {
                val dropColName = stmt.substring(stmt.indexOf("DROP COLUMN ") + 12).trim
                // loop through schema and delete sruct field matching name and type
                logInfo(s"Recoveryservice : createSchema: " +
                    s"dropcolName: $dropColName schema $schema")
                val indx = schema.fieldIndex(dropColName)
                alteredSchema = new StructType(schema.toArray.filter(
                  field => !field.name.toUpperCase.equals(dropColName.toUpperCase)))

                logInfo(s"Recoveryservice : createSchema: alteredSchema: $alteredSchema")
              }
              schema = alteredSchema

              schemaStructMap.put(s"${versionCnt}#${fqtn}", alteredSchema)
              val idArray: Array[Int] = new Array[Int](alteredSchema.fields.length)
              val prevSchema = getSchemaStructMap.getOrElse(s"${versionCnt - 1}#${fqtn}", null)
              val prevIdArray: Array[Int] =
                tableColumnIds.getOrElse(s"${versionCnt - 1}#${fqtn}", null)

              assert(prevSchema != null && prevIdArray != null)

              for (i <- 0 until alteredSchema.fields.length) {
                val prevId = prevSchema.fields.indexOf(alteredSchema.fields(i))
                idArray(i) = if (prevId == -1) {
                  // Alter Add column case
                  idArray(i - 1) + 1
                } else {
                  // Common column to previous schema
                  prevIdArray(prevId)
                }
              }
              tableColumnIds.put(s"${versionCnt}#${fqtn}", idArray)
              versionMap.put(fqtn, versionCnt)
              versionCnt += 1
            })
          }
          case None => ""
        }
      }
    })
  }
  def getNumBuckets(schemaName: String, tableName: String): (Integer, Boolean) = {
    val memberContainsRegion = memberObject
        .getPrToNumBuckets.containsKey(s"/${schemaName}/${tableName}")
    if (memberContainsRegion) {
      (memberObject.getPrToNumBuckets.get(s"/${schemaName}/${tableName}"), false)
    } else {
      if (memberObject.getreplicatedRegions().contains(s"/${schemaName}/${tableName}")) {
        logInfo("table is replicated ")
        (1, true)
      } else {
        logWarning(s"num of buckets not found for /${schemaName}/${tableName}")
        import collection.JavaConversions._
        for((k, v) <- memberObject.getPrToNumBuckets) {
          logInfo(s"PrToNumBuckets map entry = ${k} -> ${v}")
        }
        (-1, false)
      }
    }
  }

  def collectViewsAndRecoverDDLs(): Unit = {
    // Send a message to all the servers and locators to send back their
    // respective persistent state information.
    val collector = new GfxdListResultCollector(null, true)
    val msg = new RecoveredMetadataRequestMessage(collector)
    msg.executeFunction()
    val persistentData = collector.getResult
    val itr = persistentData.iterator()

    val snapCon = SnappyContext()
    val snappyHiveExternalCatalog = HiveClientUtil
        .getOrCreateExternalCatalog(snapCon.sparkContext, snapCon.sparkContext.getConf)

    while (itr.hasNext) {
      val persistentViewObj = itr.next().asInstanceOf[
          ListResultCollectorValue].resultOfSingleExecution.asInstanceOf[
          PersistentStateInRecoveryMode]
      logInfo(s"collecting ddls. persistentViewObj = ${persistentViewObj}")

      persistentObjectMemberMap += persistentViewObj.getMember -> persistentViewObj
      val regionItr = persistentViewObj.getAllRegionViews.iterator()
      while (regionItr.hasNext) {
        val x = regionItr.next();
        val regionPath = x.getRegionPath
        val set = regionViewSortedSet.get(regionPath)
        if (set.isDefined) {
          set.get += x
        } else {
          var newset = mutable.SortedSet.empty[RecoveryModePersistentView]
          newset += x
          regionViewSortedSet += regionPath -> newset
        }
      }
    }

    // Print all the set
    for ((k, v) <- regionViewSortedSet) {
      println(s"region = $k and set = $v")
    }

    // identify which members catalog object to be used
    val hiveRegionViews = regionViewSortedSet.filterKeys(
      _.startsWith(SystemProperties.SNAPPY_HIVE_METASTORE_PATH))
    val hiveRegionToConsider =
      hiveRegionViews.keySet.toSeq.sortBy(hiveRegionViews.get(_).size).reverse.head
    logInfo(s"Hive region to consider = $hiveRegionToConsider")
    val mostUptodateRegionView = regionViewSortedSet(hiveRegionToConsider).lastKey
    val memberToConsiderForHiveCatalog = mostUptodateRegionView.getMember


    val nonHiveRegionViews = regionViewSortedSet.filterKeys(
      !_.startsWith(SystemProperties.SNAPPY_HIVE_METASTORE_PATH))
    val regionToConsider =
      nonHiveRegionViews.keySet.toSeq.sortBy(nonHiveRegionViews.get(_).size).reverse.head
    logInfo(s"Non Hive region to consider = $regionToConsider")
    val regionView = regionViewSortedSet(regionToConsider).lastKey
    val memberToConsider = regionView.getMember
    memberObject = persistentObjectMemberMap(memberToConsider)

    logInfo(s"For Hive memberToConsiderForHiveCatalog = $memberToConsiderForHiveCatalog")
    for ((k, v) <- persistentObjectMemberMap) {
      logInfo(s"persistentObjectMemberMap = ${k} ${v}")
    }

    mostRecentMemberObject = persistentObjectMemberMap(memberToConsiderForHiveCatalog)
    val otherExtractedDDLs = mostRecentMemberObject.getOtherDDLs

    logInfo(s"Other extracted ddls: = $otherExtractedDDLs")

    val catalogObjects = mostRecentMemberObject.getCatalogObjects

    import scala.collection.JavaConverters._

    val catalogArr = catalogObjects.asScala.map(catObj => {
      val catalogMetadataDetails = new CatalogMetadataDetails()
      catObj match {
        case catFunObj: CatalogFunctionObject => {
          ConnectorExternalCatalog.convertToCatalogFunction(catFunObj)
        }
        case catDBObj: CatalogSchemaObject => {
          ConnectorExternalCatalog.convertToCatalogDatabase(catDBObj)
        }
        case catTabObj: CatalogTableObject => {
          ConnectorExternalCatalog.convertToCatalogTable(
            catalogMetadataDetails.setCatalogTable(catTabObj), snapCon.sparkSession)._1
        }
      }
    })

    RecoveryService.populateCatalog(catalogArr, snapCon.sparkContext)
    logInfo(" Populated catalog")

    val dbList = snappyHiveExternalCatalog.listDatabases("*")
    val allFunctions = dbList.map(dbName => snappyHiveExternalCatalog.listFunctions(dbName, "*")
        .map(func => snappyHiveExternalCatalog.getFunction(dbName, func)))
    val allDatabases = dbList.map(snappyHiveExternalCatalog.getDatabase)

    logInfo("RecoveryService: Catalog contents in recovery mode:\nTables\n"
        + snappyHiveExternalCatalog.getAllTables().toString() + "\nDatabases\n"
        + allDatabases.toString() + "\nFunctions\n" + allFunctions.toString())
    createSchemasMap(snappyHiveExternalCatalog)
  }

  def getProvider(tableName: String): String = {
    logInfo(s"RecoveryService.getProvider called with tablename $tableName and size of cto="
        + mostRecentMemberObject.getCatalogObjects.size())
    val res = mostRecentMemberObject.getCatalogObjects.asScala.filter(x => {
      x.isInstanceOf[CatalogTableObject] && {
        val cbo = x.asInstanceOf[CatalogTableObject]
        val fqtn = s"${cbo.getSchemaName}.${cbo.getTableName}"
        logInfo(s"RecoveryService.getProvider fqtn $fqtn")
        fqtn.equalsIgnoreCase(tableName)
      }
    }).head.asInstanceOf[CatalogTableObject]
    logInfo(s"RecoveryService.getProvider provider for $tableName is ${res.getProvider}")
    res.getProvider
  }

  /**
   * Populates the external catalog, in recovery mode. Currently table,function and
   * database type of catalog objects is supported.
   *
   * @param catalogObjSeq Sequence of catalog objects to be inserted in the catalog
   * @param sc            Spark Context
   */

  def populateCatalog(catalogObjSeq: Seq[Any], sc: SparkContext): Unit = {
    val extCatalog = HiveClientUtil.getOrCreateExternalCatalog(sc, sc.getConf)
    catalogObjSeq.foreach {
      case catDB: CatalogDatabase =>
        extCatalog.createDatabase(catDB, ignoreIfExists = true)
        logInfo(s"Inserting catalog database: ${catDB.name} in the catalog.")
      case catFunc: CatalogFunction =>
        extCatalog.createFunction(catFunc.identifier.database
            .getOrElse(Constant.DEFAULT_SCHEMA), catFunc)
        logInfo(s"Inserting catalog function: ${catFunc.identifier.funcName} in the catalog.")
      case catTab: CatalogTable =>
        val opLogTable = catTab.copy(provider = Option("oplog"))
        extCatalog.createTable(opLogTable, ignoreIfExists = true)
        logInfo(s"Inserting catalog table: ${catTab.identifier.table} in the catalog.")
    }
  }
}

object RegionDiskViewOrdering extends Ordering[RecoveryModePersistentView] {
  def compare(element1: RecoveryModePersistentView,
      element2: RecoveryModePersistentView): Int = {
    element2.compareTo(element1)
  }
}
