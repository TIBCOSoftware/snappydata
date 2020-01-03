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

import java.util.function.BiConsumer

import com.pivotal.gemfirexd.internal.engine.ui.{
  SnappyExternalTableStats, SnappyIndexStats,
  SnappyRegionStats
}
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper
import com.gemstone.gemfire.internal.shared.SystemProperties
import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.ddl.{
  DDLConflatable, GfxdDDLQueueEntry,
  GfxdDDLRegionQueue
}
import com.pivotal.gemfirexd.internal.engine.distributed.RecoveryModeResultCollector
import com.pivotal.gemfirexd.internal.engine.distributed.message.PersistentStateInRecoveryMode
import com.pivotal.gemfirexd.internal.engine.distributed.message.PersistentStateInRecoveryMode
.{RecoveryModePersistentView, RecoveryModePersistentViewPair}
import com.pivotal.gemfirexd.internal.engine.sql.execute.RecoveredMetadataRequestMessage
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.util.Random

import com.gemstone.gemfire.distributed.internal.DistributionManager
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.impl.jdbc.Util
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.Constant

import org.apache.spark.sql.{SnappyContext, SnappyParser, SnappySession}
import io.snappydata.sql.catalog.ConnectorExternalCatalog
import io.snappydata.thrift.{
  CatalogFunctionObject, CatalogMetadataDetails, CatalogSchemaObject,
  CatalogTableObject
}

import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogFunction, CatalogTable}
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructField, StructType}
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveClientUtil
import org.apache.spark.sql.internal.ContextJarUtils

object RecoveryService extends Logging {
  var recoveryStats: (
      Seq[SnappyRegionStats], Seq[SnappyIndexStats], Seq[SnappyExternalTableStats]) = _
  var catalogTableCount = 0
  val snappyHiveExternalCatalog = HiveClientUtil
      .getOrCreateExternalCatalog(SnappyContext().sparkContext, SnappyContext().sparkContext
          .getConf)
  var enableTableCountInUI: Boolean = _

  private def isGrantRevokeStatement(conflatable: DDLConflatable) = {
    val sqlText = conflatable.getValueToConflate
    // return (sqlText != null && GRANTREVOKE_PATTERN.matcher(sqlText).matches());
    sqlText != null && (sqlText.toUpperCase.startsWith("GRANT")
        || sqlText.toUpperCase.startsWith("REVOKE"))
  }

  def getStats: (Seq[SnappyRegionStats], Seq[SnappyIndexStats], Seq[SnappyExternalTableStats]) = {
    if (recoveryStats == null || catalogTableCount != getTables.size) {
      val snappyContext = SnappyContext()
      val snappySession: SnappySession = snappyContext.snappySession
      if (Misc.isSecurityEnabled) {
        snappySession.conf.set(Attribute.USERNAME_ATTR,
          Misc.getMemStore.getBootProperty(Attribute.USERNAME_ATTR))
        snappySession.conf.set(Attribute.PASSWORD_ATTR,
          Misc.getMemStore.getBootProperty(Attribute.PASSWORD_ATTR))
      }
      val allTables = getTables
      var tblCounts: Seq[SnappyRegionStats] = Seq()
      catalogTableCount = allTables.length
      allTables.foreach(table => {
        try {
          table.storage.locationUri match {
            case Some(_) => // external tables can be seen in show tables but filtered out in UI
            case None =>
              if (recoveryStats == null ||
                  !recoveryStats._1.map(_.getTableName).contains(table.qualifiedName))
                logDebug(s"Querying table: $table for count")
              val recCount = if (enableTableCountInUI) {
                snappySession.sql(s"SELECT count(1) FROM ${table.qualifiedName}")
                    .collect()(0).getLong(0)
              } else -1L
              val (numBuckets, isReplicatedTable) = RecoveryService
                  .getNumBuckets(table.qualifiedName.split('.')(0).toUpperCase(),
                    table.qualifiedName.split('.')(1).toUpperCase())
              val regionStats = new SnappyRegionStats()
              regionStats.setRowCount(recCount)
              regionStats.setTableName(table.qualifiedName)
              regionStats.setReplicatedTable(isReplicatedTable)
              regionStats.setBucketCount(numBuckets)
              regionStats.setColumnTable(getProvider(table.qualifiedName).equalsIgnoreCase
              ("column"))
              tblCounts :+= regionStats
          }
        } catch {
          case e: Exception => logError(s"Error querying table $table.\n$e")
            val regionStats = new SnappyRegionStats()
            regionStats.setRowCount(-1L)
            regionStats.setTableName(table.qualifiedName)
            regionStats.setColumnTable(getProvider(table.qualifiedName).equalsIgnoreCase("column"))
            tblCounts :+= regionStats
        }
      })
      recoveryStats = (tblCounts, Seq(), Seq())
    }
    recoveryStats
  }

  def getAllDDLs: mutable.Buffer[String] = {
    val ddlBuffer: mutable.Buffer[String] = List.empty.toBuffer

    if (!Misc.getGemFireCache.isSnappyRecoveryMode) {
      val dd = Misc.getMemStore.getDatabase.getDataDictionary
      try {
        if (dd == null) {
          throw Util.generateCsSQLException(SQLState.SHUTDOWN_DATABASE, Attribute.GFXD_DBNAME)
        }
        dd.lockForReadingRT(null)
        val ddlQ = new GfxdDDLRegionQueue(Misc.getMemStore.getDDLStmtQueue.getRegion)
        ddlQ.initializeQueue(dd)
        val allDDLs: java.util.List[GfxdDDLQueueEntry] = ddlQ.peekAndRemoveFromQueue(-1, -1)
        val preProcessedqueue = ddlQ.getPreprocessedDDLQueue(
          allDDLs, null, null, null, false).iterator

        import scala.collection.JavaConversions._
        for (queueEntry <- preProcessedqueue) {
          queueEntry.getValue match {
            case conflatable: DDLConflatable =>
              val schema = conflatable.getSchemaForTableNoThrow
              if (conflatable.isCreateDiskStore) {
                ddlBuffer.add(conflatable.getValueToConflate)
              } else if (Misc.SNAPPY_HIVE_METASTORE == schema ||
                  Misc.SNAPPY_HIVE_METASTORE == conflatable.getCurrentSchema ||
                  Misc.SNAPPY_HIVE_METASTORE == conflatable.getRegionToConflate) {
              } else if (conflatable.isAlterTable || conflatable.isCreateIndex ||
                  isGrantRevokeStatement(conflatable) ||
                  conflatable.isCreateTable || conflatable.isDropStatement ||
                  conflatable.isCreateSchemaText) {
                val ddl = conflatable.getValueToConflate
                val ddlLowerCase = ddl.toLowerCase()
                if ("create[ ]+diskstore".r.findFirstIn(ddlLowerCase).isDefined ||
                    "create[ ]+index".r.findFirstIn(ddlLowerCase).isDefined ||
                    ddlLowerCase.trim.contains("^grant") ||
                    ddlLowerCase.trim.contains("^revoke")) {
                  ddlBuffer.add(ddl)
                }
              }
            case _ =>
          }
        }
      } finally {
        dd.unlockAfterReading(null)
      }
    } else {
      val otherDdls = mostRecentMemberObject.getOtherDDLs.asScala
      otherDdls.foreach(ddl => {
        val ddlLowerCase = ddl.toLowerCase()
        if ("create[ ]+diskstore".r.findFirstIn(ddlLowerCase).isDefined ||
            "create[ ]+index".r.findFirstIn(ddlLowerCase).isDefined ||
            ddlLowerCase.trim.contains("^grant") ||
            ddlLowerCase.trim.contains("^revoke")) {
          ddlBuffer.append(ddl)
        }
      })
    }
    val dbList = snappyHiveExternalCatalog.listDatabases("*").filter(dbName =>
      !(dbName.equalsIgnoreCase("SYS") || dbName.equalsIgnoreCase("DEFAULT")))

    val allFunctions = dbList.flatMap(dbName => snappyHiveExternalCatalog
        .listFunctions(dbName, "*")
        .map(func => snappyHiveExternalCatalog.getFunction(dbName, func)))
    val allDatabases = dbList.map(snappyHiveExternalCatalog.getDatabase)
    allFunctions.foreach(func => {
      val funcClass = func.className.split("__")(0)
      val funcRetType = func.className.split("__")(1)
      assert(func.resources.map(_.uri).length == 1, "Function resource should be singular.")
      val funcDdl = s"CREATE FUNCTION ${func.identifier} " +
          s"AS $funcClass RETURNS $funcRetType  USING JAR '${func.resources.map(_.uri).head}'"
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
      val allkeys = table.properties.keys.toSeq

      // ======= covers create statements ======================
      logDebug(s"RecoveryService: getAllDDLs: table: $table")
      val numPartsNTSKey = allkeys.filter(f => f.contains("numPartsOrgSqlText"))

      if (numPartsNTSKey.nonEmpty) {
        val numPartsNTS = numPartsNTSKey.head.split("_")
        val numSqlTextParts = table.properties.getOrElse(numPartsNTSKey.head, "0").toInt
        assert(numSqlTextParts != 0,
          s"numPartsOrgSqlText key not found in catalog table properties for" +
              s" table ${table.identifier}.")
        val createTimestamp = numPartsNTS(1).toLong
        val createTableString = (0 until numSqlTextParts).map { i =>
          table.properties.getOrElse(s"sqlTextpart.$i", null)
        }.mkString
        tempTableBuffer append ((s"createTableString_$createTimestamp", createTableString))
      }
      // ========== covers alter statements ================
      allkeys.filter(f => f.contains("altTxt")).foreach(key =>
        tempTableBuffer append ((key, table.properties(key))))

      // ============ covers view statements ============
      table.viewOriginalText match {
        case Some(ddl) => tempViewBuffer.append(s"create view ${table.identifier} as $ddl")
        case None => ""
      }
    })

    // the sorting is required to arrange statements amongst tables
    val sortedTempTableBuffer =
      tempTableBuffer.sortBy(tup => tup._1.split("_")(1).toLong).map(_._2)
    ddlBuffer.appendAll(sortedTempTableBuffer)

    // view ddls should be at the end so that the extracted ddls won't fail when replayed as is.
    ddlBuffer.appendAll(tempViewBuffer)

    val biConsumer = new BiConsumer[String, Object] {
      def accept(alias: String, cmdObj: Object): Unit = {
        if (!(alias.equals(Constant.CLUSTER_ID) ||
            alias.startsWith(Constant.MEMBER_ID_PREFIX) ||
            !cmdObj.isInstanceOf[String])) {
          val cmd = cmdObj.asInstanceOf[String]
          logInfo("#RecoveryService " + alias + cmd)
          val cmdFields = cmd.split("\\|", -1)
          if (cmdFields.length > 1) {
            val repos = cmdFields(1)
            val path = cmdFields(2)
            if (!repos.isEmpty && !path.isEmpty) {
              ddlBuffer
                  .append(s"DEPLOY PACKAGE $alias '${cmdFields(0)}' repos '$repos' path '$path'")
            }
            else if (!repos.isEmpty && path.isEmpty) {
              ddlBuffer.append(s"DEPLOY PACKAGE $alias '${cmdFields(0)}' repos '$repos'")
            }
            else if (repos.isEmpty && !path.isEmpty) {
              ddlBuffer.append(s"DEPLOY PACKAGE $alias '${cmdFields(0)}' path '$path'")
            }
            else {
              ddlBuffer.append(s"DEPLOY PACKAGE $alias '${cmdFields(0)}'")
            }
          } else {
            if (!(alias.contains(ContextJarUtils.functionKeyPrefix) ||
                alias.contains(ContextJarUtils.droppedFunctionsKey))) {
              ddlBuffer.append(s"DEPLOY JAR $alias '${cmdFields(0)}'")
            }
          }
        }
      }
    }
    Misc.getMemStore.getMetadataCmdRgn.forEach(biConsumer)
    ddlBuffer
  }

  def getColRegionPath(schemaName: String, tableName: String, bucketNumber: Int): String = {
    val internalFQTN = schemaName + '.' + Constant.SHADOW_SCHEMA_NAME_WITH_SEPARATOR +
        tableName + Constant.SHADOW_TABLE_SUFFIX

    if (Misc.getRegionPath(internalFQTN.toUpperCase) == null) {
      throw new IllegalStateException(s"regionPath for $internalFQTN not found")
    } else {
      val regionPath = Misc.getRegionPath(internalFQTN.toUpperCase)
      s"/_PR//B_${regionPath.substring(1, regionPath.length - 1)}/_${bucketNumber}"
    }
  }

  def getRowRegionPath(fqtn: String, bucketId: Int): String = {
    val schemaName = fqtn.split('.')(0)
    val tableName = fqtn.split('.')(1)
    val replicated = isReplicated(schemaName, tableName)
    // using row region path as column regions will be colocated on same host
    val tablePath = '/' + fqtn.replace(".", "/")
    var bucketPath = tablePath
    if (!replicated) {
      val bucketName = PartitionedRegionHelper.getBucketName(tablePath, bucketId)
      bucketPath = s"/${PartitionedRegionHelper.PR_ROOT_REGION_NAME}/$bucketName"
    }
    bucketPath
  }

  /* fqtn and bucket number for PR r Column table, -1 indicates replicated row table */
  def getExecutorHost(fqtn: String, bucketId: Int = -1): Seq[String] = {
    val rowRegionPath = getRowRegionPath(fqtn, bucketId)
    // for null region maps select random host
    val rowRegionPathKey = combinedViewsMapSortedSet.keySet.filter(_.contains(rowRegionPath + "#$"))

    if (rowRegionPathKey.size != 0) {
      assert(rowRegionPathKey.size == 1,
        s"combinedViewsMapSortedSet should not contain multiple entries for the key $rowRegionPath")
      val viewPair = combinedViewsMapSortedSet(rowRegionPathKey.head).lastKey
      val hostCanonical = viewPair.getRowView.getExecutorHost
      val host = hostCanonical.split('(').head
      logDebug(s"Host chosen for bucket : ${rowRegionPathKey.head}  :" +
          s"  ${Seq(s"executor_${host}_$hostCanonical")}")
      Seq(s"executor_${host}_$hostCanonical")
    } else {
      logWarning(s"Preferred host is not found for bucket id: $bucketId. " +
          s"Choosing random host for $rowRegionPath")
      getRandomExecutorHost
    }
  }

  def getRandomExecutorHost: Seq[String] = {
    val randomPersistentView = Random.shuffle(regionViewMapSortedSet.toList).head._2.firstKey
    val hostCanonical = randomPersistentView.getExecutorHost
    val host = hostCanonical.split('(').head
    Seq(s"executor_${host}_$hostCanonical")
  }

  val regionViewMapSortedSet: mutable.Map[String,
      mutable.SortedSet[RecoveryModePersistentView]] = mutable.Map.empty

  val combinedViewsMapSortedSet: mutable.Map[String,
      mutable.SortedSet[RecoveryModePersistentViewPair]] = mutable.Map.empty

  var locatorMember: InternalDistributedMember = _
  val persistentObjectMemberMap: mutable.Map[
      InternalDistributedMember, PersistentStateInRecoveryMode] = mutable.Map.empty
  var mostRecentMemberObject: PersistentStateInRecoveryMode = _
  var memberObject: PersistentStateInRecoveryMode = _
  var schemaStructMap: mutable.Map[String, StructType] = mutable.Map.empty
  val versionMap: mutable.Map[String, Int] = collection.mutable.Map.empty
  val tableColumnIds: mutable.Map[String, Array[Int]] = mutable.Map.empty

  def updateMetadataMaps(fqtnKey: String, alteredSchema: StructType, versionCnt: Int): Int = {
    schemaStructMap.put(s"$versionCnt#$fqtnKey", alteredSchema)
    val idArray: Array[Int] = new Array[Int](alteredSchema.fields.length)
    val prevSchema = schemaStructMap.getOrElse(s"${versionCnt - 1}#$fqtnKey", null)
    val prevIdArray: Array[Int] =
      tableColumnIds.getOrElse(s"${versionCnt - 1}#$fqtnKey", null)
    assert(prevSchema != null && prevIdArray != null)
    for (i <- alteredSchema.fields.indices) {
      val prevFieldIndex = prevSchema.fields.indexOf(alteredSchema.fields(i))
      idArray(i) = if (prevFieldIndex == -1) {
        // Alter Add column case
        idArray(i - 1) + 1
      } else {
        // Common column to previous schema
        prevIdArray(prevFieldIndex)
      }
    }
    // idArray contains column ids from alter statement
    tableColumnIds.put(s"$versionCnt#$fqtnKey", idArray)
    versionMap.put(fqtnKey, versionCnt)
    versionCnt + 1
  }

  def createSchemasMap: Unit = {
    val snappySession = new SnappySession(SnappyContext().sparkContext)
    val colParser = new SnappyParser(snappySession)
    getTables.foreach(table => {
      // Create statements
      var versionCnt = 1
      val numOfSchemaParts = table.properties.getOrElse("NoOfschemaParts", "0").toInt
      assert(numOfSchemaParts != 0, "Schema string not found.")
      val schemaJsonStr = (0 until numOfSchemaParts)
          .map { i => table.properties.getOrElse(s"schemaPart.$i", null) }.mkString
      logDebug(s"RecoveryService: createSchemaMap: Schema Json String : $schemaJsonStr")
      val fqtnKey = table.identifier.database match {
        case Some(schName) => schName + "_" + table.identifier.table
        case None => throw new Exception(
          s"Schema name not found for the table ${table.identifier.table}")
      }
      var schema: StructType = StructType(DataType.fromJson(schemaJsonStr).asInstanceOf[StructType]
          .map(f => f.copy(name = f.name.toLowerCase)))

      assert(schema != null, s"schemaJson read from catalog table is null " +
          s"for ${table.identifier.table}")
      schemaStructMap.put(s"$versionCnt#$fqtnKey", schema)
      // for a table created with schema c1, c2 then c1 is dropped and then c1 is added
      // the added c1 is a new column and we want to be able to differentiate between both c1
      // so we assign ids to columns and new ids to columns from alter commands
      // tableColumnIds = Map("1#fqtn" -> Array(0, 1)). Entry "2#fqtn" -> Array(1, 2)
      // will be added when alter/add are identified for this table
      tableColumnIds.put(s"$versionCnt#$fqtnKey", schema.fields.indices.toArray)
      versionMap.put(fqtnKey, versionCnt)
      versionCnt += 1

      // Alter statements contains original sql with timestamp in keys to find sequence.for example
      // Properties: [k1=v1, altTxt_1568129777251=<alter command>, k3=v3]
      val altStmtKeysSorted = table.properties.keys
          .filter(_.contains(s"altTxt_")).toSeq
          .sortBy(_.split("_")(1).toLong)
      altStmtKeysSorted.foreach(k => {
        val stmt = table.properties(k).toUpperCase()
        var alteredSchema: StructType = null
        if (stmt.contains(" ADD COLUMN ")) {
          val columnString = stmt.substring(stmt.indexOf("ADD COLUMN ") + 11)
          val colNameAndType = (columnString.split("[ ]+")(0), columnString.split("[ ]+")(1))
          // along with not null other column definitions should be handled
          val colString = if (columnString.toLowerCase().contains("not null")) {
            s"(${colNameAndType._1} ${colNameAndType._2}) not null"
          } else s"(${colNameAndType._1} ${colNameAndType._2})"
          val field = colParser.parseSQLOnly(colString, colParser.tableSchemaOpt.run())
          match {
            case Some(fieldSeq) =>
              val field = fieldSeq.head
              val builder = new MetadataBuilder
              builder.withMetadata(field.metadata)
                  .putString("originalSqlType", colNameAndType._2.trim.toLowerCase())
              StructField(field.name, field.dataType, field.nullable, builder.build())
            case None => throw
                new IllegalArgumentException("alter statement contains no parsable field")
          }
          alteredSchema = new StructType((schema ++ new StructType(Array(field))).toArray)
          schema = alteredSchema
          assert(schema != null, s"schema for version $versionCnt is null")
          versionCnt = updateMetadataMaps(fqtnKey, alteredSchema, versionCnt)
        } else if (stmt.contains("DROP COLUMN ")) {
          val dropColName = stmt.substring(stmt.indexOf("DROP COLUMN ") + 12).trim
          // loop through schema and delete sruct field matching name and type

          alteredSchema = new StructType(
            schema.toArray.filter(!_.name.equalsIgnoreCase(dropColName)))
          schema = alteredSchema
          assert(schema != null, s"schema for version $versionCnt is null")
          versionCnt = updateMetadataMaps(fqtnKey, alteredSchema, versionCnt)
        } else {
          // do nothing in case of "alter table add constraint"
        }
      })
    })
    logDebug(
      s"""SchemaVsVersionMap:
         |$schemaStructMap
         |
         |TableVsColumnIDsMap:
         |$tableColumnIds
         |
         |TableVersionMap
         |$versionMap""".stripMargin)
  }


  def getNumBuckets(schemaName: String, tableName: String): (Integer, Boolean) = {
    val prName = s"/$schemaName/$tableName"
    val memberContainsRegion = memberObject
        .getPrToNumBuckets.containsKey(prName)
    if (memberContainsRegion) {
      (memberObject.getPrToNumBuckets.get(prName), false)
    } else {
      if (memberObject.getReplicatedRegions.contains(prName)) {
        (1, true)
      } else {
        logWarning(s"Number of buckets for $prName not found in ${memberObject.getMember}")
        (0, false)
      }
    }
  }

  def isReplicated(schemaName: String, tableName: String): Boolean =
    memberObject.getReplicatedRegions.contains(s"/$schemaName/$tableName")

  def collectViewsAndPrepareCatalog(enableTableCountInUI: Boolean): Unit = {
    // Send a message to all the servers and locators to send back their
    // respective persistent state information.
    logDebug("Start collecting PersistentStateInRecoveryMode from all the servers/locators.")
    this.enableTableCountInUI = enableTableCountInUI
    val collector = new RecoveryModeResultCollector()
    val msg = new RecoveredMetadataRequestMessage(collector)
    msg.executeFunction()
    val persistentData = collector.getResult
    logDebug(s"Total Number of PersistentStateInRecoveryMode objects received" +
        s" ${persistentData.size()}")
    val itr = persistentData.iterator()
    while (itr.hasNext) {
      val persistentStateObj = itr.next().asInstanceOf[PersistentStateInRecoveryMode]
      locatorMember = if (persistentStateObj.getMember.getVmKind ==
          DistributionManager.LOCATOR_DM_TYPE && locatorMember == null) {
        persistentStateObj.getMember
      }
      else if (persistentStateObj.getMember.getVmKind ==
          DistributionManager.LOCATOR_DM_TYPE && locatorMember != null) {
        persistentStateObj.getMember
      }
      else {
        locatorMember
      }
      persistentObjectMemberMap += persistentStateObj.getMember -> persistentStateObj
      val regionItr = persistentStateObj.getAllRegionViews.iterator()
      while (regionItr.hasNext) {
        val persistentView = regionItr.next()
        val regionPath = persistentView.getRegionPath
        val set = regionViewMapSortedSet.get(regionPath)
        if (set.isDefined) {
          set.get += persistentView
        } else {
          var newset = mutable.SortedSet.empty[RecoveryModePersistentView]
          newset += persistentView
          regionViewMapSortedSet += regionPath -> newset
        }
      }
    }
    assert(locatorMember != null)
    mostRecentMemberObject = persistentObjectMemberMap(locatorMember)
    logDebug(s"The selected PersistentStateInRecoveryMode used for populating" +
        s" the new catalog:\n$mostRecentMemberObject")

    val catalogObjects = mostRecentMemberObject.getCatalogObjects
    import scala.collection.JavaConverters._
    val catalogArr = catalogObjects.asScala.map(catObj => {
      val catalogMetadataDetails = new CatalogMetadataDetails()
      catObj match {
        case catFunObj: CatalogFunctionObject =>
          ConnectorExternalCatalog.convertToCatalogFunction(catFunObj)
        case catDBObj: CatalogSchemaObject =>
          ConnectorExternalCatalog.convertToCatalogDatabase(catDBObj)
        case catTabObj: CatalogTableObject =>
          ConnectorExternalCatalog.convertToCatalogTable(
            catalogMetadataDetails.setCatalogTable(catTabObj), SnappyContext().sparkSession)._1
      }
    })
    RecoveryService.populateCatalog(catalogArr)

    // Identify best member to get bucket-related info, etc
    val nonHiveRegionViews = regionViewMapSortedSet.filterKeys(
      !_.startsWith(SystemProperties.SNAPPY_HIVE_METASTORE_PATH))
    if (nonHiveRegionViews.isEmpty) {
      logError("No relevant RecoveryModePersistentViews found.")
      throw new Exception("Cannot start empty cluster in Recovery Mode.")
    }
    val memberToConsider = persistentObjectMemberMap.keySet.toSeq.sortBy(e =>
      persistentObjectMemberMap(e).getPrToNumBuckets.size() +
          persistentObjectMemberMap(e).getReplicatedRegions.size()).last
    memberObject = persistentObjectMemberMap(memberToConsider)
    logDebug(s"The selected non-Hive PersistentStateInRecoveryMode : $memberObject")
    createSchemasMap
    createCombinedSortedSet

    val dbList = snappyHiveExternalCatalog.listDatabases("*")
    val allFunctions = dbList.map(dbName =>
      snappyHiveExternalCatalog.listFunctions(dbName, "*")
          .map(func => snappyHiveExternalCatalog.getFunction(dbName, func)))
    val allDatabases = dbList.map(snappyHiveExternalCatalog.getDatabase)
    logInfo(
      s"""Catalog contents in recovery mode:
         |Tables:
         |${snappyHiveExternalCatalog.getAllTables().toString()}
         |Databases:
         |${allDatabases.toString()}
         |Functions:
         |${allFunctions.toString()}""".stripMargin)
  }

  def createCombinedSortedSet: Unit = {
    snappyHiveExternalCatalog.getAllTables().foreach(table => {
      val tableName = table.identifier.table
      val schemaName = table.identifier.database.getOrElse("app")
      val fqtn = (schemaName + "." + tableName).toUpperCase()
      val tableType = getProvider(fqtn.toLowerCase())
      val numBuckets = getNumBuckets(schemaName.toUpperCase(), tableName.toUpperCase())._1
      var rowRegionPath: String = ""
      var colRegionPath: String = ""
      for (buckNum <- Range(0, numBuckets)) {

        rowRegionPath = getRowRegionPath(fqtn, buckNum)
        colRegionPath = if (tableType == "column") {
          PartitionedRegionHelper
              .escapePRPath(getColRegionPath(schemaName, tableName, buckNum))
              .replaceFirst("___PR__B__", "/__PR/_B__")
        } else ""

        val ssKey = if (colRegionPath.isEmpty) {
          rowRegionPath + "#$"
        } else {
          rowRegionPath + "#$" + colRegionPath
        }
        if (regionViewMapSortedSet.contains(rowRegionPath)) {
          val rowViews = regionViewMapSortedSet(rowRegionPath)
          val colViews =
            if (colRegionPath.nonEmpty) regionViewMapSortedSet(colRegionPath)
            else mutable.SortedSet.empty[RecoveryModePersistentView]

          for (rowView <- rowViews) {
            if (tableType == "column" && colViews.nonEmpty) {
              for (colView <- colViews) {
                if (rowView.getMember.equals(colView.getMember)) {
                  val set = combinedViewsMapSortedSet.get(ssKey)
                  if (set.isDefined) {
                    set.get += new RecoveryModePersistentViewPair(rowView, colView)
                  } else {
                    var newSet = mutable.SortedSet.empty[RecoveryModePersistentViewPair]
                    newSet += new RecoveryModePersistentViewPair(rowView, colView)
                    combinedViewsMapSortedSet += ssKey -> newSet
                  }
                }
              }
            } else {
              // case: when its a row table
              val set = combinedViewsMapSortedSet.get(ssKey)
              if (set.isDefined) {
                set.get += new RecoveryModePersistentViewPair(rowView, null)
              } else {
                var newSet = mutable.SortedSet.empty[RecoveryModePersistentViewPair]
                newSet += new RecoveryModePersistentViewPair(rowView, null)
                combinedViewsMapSortedSet += ssKey -> newSet
              }
            }
          }
        }
      }
    })

    logDebug("Printing combinedViewsMapSortedSet")
    for (elem <- combinedViewsMapSortedSet) {
      logDebug(s"bucket:${elem._1} :::::: Set size:${elem._2.size}\n")
      for (viewPair <- elem._2) {
        logDebug(s"Row View : ${viewPair.getRowView.getMember}")
        if (viewPair.getColView != null)
          logDebug(s"Col View : ${viewPair.getColView.getMember}")
        logDebug("* ---------- *")
      }
      logDebug("\n\n\n")
    }
  }

  def getTables: Seq[CatalogTable] = snappyHiveExternalCatalog.getAllTables()
      .filter(!_.tableType.name.equalsIgnoreCase("view"))

  def getProvider(tableName: String): String = {
    logDebug(s"RecoveryService: tableName: $tableName")
    val res = mostRecentMemberObject.getCatalogObjects.asScala.filter(x => {
      x.isInstanceOf[CatalogTableObject] && {
        val cbo = x.asInstanceOf[CatalogTableObject]
        val fqtn = s"${
          cbo.getSchemaName
        }.${
          cbo.getTableName
        }"
        fqtn.equalsIgnoreCase(tableName)
      }
    }).head.asInstanceOf[CatalogTableObject]
    logDebug(s"Provider: ${res.getProvider}")
    res.getProvider
  }

  /**
   * Populates the external catalog, in recovery mode. Currently table,function and
   * database type of catalog objects is supported.
   *
   * @param catalogObjSeq Sequence of catalog objects to be inserted in the catalog
   */
  def populateCatalog(catalogObjSeq: Seq[Any]): Unit = {
    val extCatalog = snappyHiveExternalCatalog
    catalogObjSeq.foreach {
      case catDB: CatalogDatabase =>
        extCatalog.createDatabase(catDB, ignoreIfExists = true)
        logDebug(s"Inserting catalog database: ${
          catDB.name
        } in the catalog.")
      case catFunc: CatalogFunction =>
        extCatalog.createFunction(catFunc.identifier.database
            .getOrElse(Constant.DEFAULT_SCHEMA), catFunc)
        logDebug(s"Inserting catalog function: ${
          catFunc.identifier
        } in the catalog.")
      case catTab: CatalogTable =>
        val opLogTable = catTab.copy(provider = Option("oplog"))
        extCatalog.createTable(opLogTable, ignoreIfExists = true)
        logDebug(s"Inserting catalog table: ${
          catTab.identifier
        } in the catalog.")
    }
  }
}

object RegionDiskViewOrdering extends Ordering[RecoveryModePersistentView] {
  def compare(element1: RecoveryModePersistentView,
      element2: RecoveryModePersistentView): Int = {
    element2.compareTo(element1)
  }
}
