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
import com.pivotal.gemfirexd.internal.engine.ui.{SnappyExternalTableStats, SnappyIndexStats, SnappyRegionStats}
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper
import com.gemstone.gemfire.internal.shared.SystemProperties
import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.ddl.{DDLConflatable, GfxdDDLQueueEntry, GfxdDDLRegionQueue}
import com.pivotal.gemfirexd.internal.engine.distributed.RecoveryModeResultCollector
import com.pivotal.gemfirexd.internal.engine.distributed.message.PersistentStateInRecoveryMode
import com.pivotal.gemfirexd.internal.engine.distributed.message.PersistentStateInRecoveryMode.RecoveryModePersistentView
import com.pivotal.gemfirexd.internal.engine.sql.execute.RecoveredMetadataRequestMessage
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.util.Random

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
import org.apache.spark.sql.internal.ContextJarUtils

object RecoveryService extends Logging {
  var recoveryStats: (
      Seq[SnappyRegionStats], Seq[SnappyIndexStats], Seq[SnappyExternalTableStats]) = _

  private def isGrantRevokeStatement(conflatable: DDLConflatable) = {
    val sqlText = conflatable.getValueToConflate
    // return (sqlText != null && GRANTREVOKE_PATTERN.matcher(sqlText).matches());
    sqlText != null && (sqlText.toUpperCase.startsWith("GRANT")
        || sqlText.toUpperCase.startsWith("REVOKE"))
  }

  def getStats: (Seq[SnappyRegionStats], Seq[SnappyIndexStats], Seq[SnappyExternalTableStats]) = {
    if (recoveryStats == null) {
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
      allTables.foreach(table => {
        table.storage.locationUri match {
          case Some(_) => // external tables can be seen in show tables but filtered out in UI
          case None =>
            logDebug(s"Querying table: $table for count")
            val recCount = snappySession.sql(s"SELECT count(1) FROM ${table.qualifiedName}")
                .collect()(0).getLong(0)
            val (numBuckets, isReplicated) = RecoveryService
                .getNumBuckets(table.qualifiedName.split('.')(0), table.qualifiedName.split('.')(1))
            val regionStats = new SnappyRegionStats()
            regionStats.setRowCount(recCount)
            regionStats.setTableName(table.qualifiedName)
            regionStats.setReplicatedTable(isReplicated)
            regionStats.setBucketCount(numBuckets)
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

    val snappyContext = SnappyContext()
    val snappyHiveExternalCatalog = HiveClientUtil
        .getOrCreateExternalCatalog(snappyContext.sparkContext, snappyContext.sparkContext.getConf)
    val dbList = snappyHiveExternalCatalog.listDatabases("*").filter(dbName =>
      !(dbName.equalsIgnoreCase("SYS") || dbName.equalsIgnoreCase("DEFAULT")))

    val allFunctions = dbList.flatMap(dbName => snappyHiveExternalCatalog.listFunctions(dbName, "*")
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
    val sortedTempTableBuffer = tempTableBuffer.sortBy(tup => tup._1.split("_")(1).toLong).map(_._2)
    ddlBuffer.appendAll(sortedTempTableBuffer)

    // view ddls should be at the end so that the extracted ddls won't fail when replayed as is.
    ddlBuffer.appendAll(tempViewBuffer)

    val biConsumer = new BiConsumer[String, String] {
      def accept(alias: String, cmd: String): Unit = {
        val cmdFields = cmd.split("\\|", -1)
        if (cmdFields.length > 1) {
          val repos = cmdFields(1)
          val path = cmdFields(2)
          if (!repos.isEmpty && !path.isEmpty) {
            ddlBuffer.append(s"DEPLOY PACKAGE $alias '${cmdFields(0)}' repos '$repos' path '$path'")
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
    Misc.getMemStore.getGlobalCmdRgn.forEach(biConsumer)
    ddlBuffer
  }

  /* fqtn and bucket number for PR r Column table, -1 indicates replicated row table */
  def getExecutorHost(fqtn: String, bucketId: Int = -1): Seq[String] = {
    // Expecting table in the format fqtn i.e. schemaname.tablename
    val schemaName = fqtn.split('.')(0)
    val tableName = fqtn.split('.')(1)
    val (_, isReplicated) = getNumBuckets(schemaName, tableName)
    // using row region path as column regions will be colocated on same host
    val tablePath = '/' + fqtn.replace(".", "/")
    var bucketPath = tablePath
    if (!isReplicated) {
      val bucketName = PartitionedRegionHelper.getBucketName(tablePath, bucketId)
      bucketPath = s"/${PartitionedRegionHelper.PR_ROOT_REGION_NAME}/$bucketName"
    }
    // for null region maps select random host
    if (regionViewSortedSet.contains(bucketPath)) {
      regionViewSortedSet(bucketPath).map(e => {
        val hostCanonical = e.getExecutorHost
        val host = hostCanonical.split('(').head
        s"executor_${host}_$hostCanonical"
      }).toSeq
    } else {
      logWarning(s"Preferred host is not found for bucket id: $bucketId. " +
          s"Choosing random host for $bucketPath")
      getRandomExecutorHost
    }
  }

  def getRandomExecutorHost: Seq[String] = {
    val e = Random.shuffle(regionViewSortedSet.toList).head._2.firstKey
    val hostCanonical = e.getExecutorHost
    val host = hostCanonical.split('(').head
    Seq(s"executor_${host}_$hostCanonical")
  }

  val regionViewSortedSet: mutable.Map[String,
      mutable.SortedSet[RecoveryModePersistentView]] = mutable.Map.empty
  val persistentObjectMemberMap: mutable.Map[
      InternalDistributedMember, PersistentStateInRecoveryMode] = mutable.Map.empty
  var mostRecentMemberObject: PersistentStateInRecoveryMode = _
  var memberObject: PersistentStateInRecoveryMode = _
  var schemaStructMap: mutable.Map[String, StructType] = mutable.Map.empty
  val versionMap: mutable.Map[String, Int] = collection.mutable.Map.empty
  val tableColumnIds: mutable.Map[String, Array[Int]] = mutable.Map.empty

  def createSchemasMap(snappyHiveExternalCatalog: SnappyHiveExternalCatalog): Unit = {
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
      var schema: StructType = DataType.fromJson(schemaJsonStr).asInstanceOf[StructType]

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

      // Alter statements contains original sql with timestamp in keys to find sequence. for example
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
        } else if (stmt.contains("DROP COLUMN ")) {
          val dropColName = stmt.substring(stmt.indexOf("DROP COLUMN ") + 12).trim
          // loop through schema and delete sruct field matching name and type

          alteredSchema = new StructType(
            schema.toArray.filter(!_.name.equalsIgnoreCase(dropColName)))
        }
        schema = alteredSchema
        assert(schema != null, s"schema for version $versionCnt is null")
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
        versionCnt += 1
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
    val prName = s"/$schemaName/$tableName".toUpperCase()
    val memberContainsRegion = memberObject
        .getPrToNumBuckets.containsKey(prName)
    if (memberContainsRegion) {
      (memberObject.getPrToNumBuckets.get(prName), false)
    } else {
      if (memberObject.getReplicatedRegions.contains(prName)) {
        (1, true)
      } else (-1, false)
    }
  }

  def collectViewsAndPrepareCatalog(): Unit = {
    // Send a message to all the servers and locators to send back their
    // respective persistent state information.
    logDebug("Start collecting PersistentStateInRecoveryMode from all the servers/locators.")
    val collector = new RecoveryModeResultCollector()
    val msg = new RecoveredMetadataRequestMessage(collector)
    msg.executeFunction()
    val persistentData = collector.getResult
    logDebug(s"Number of PersistentStateInRecoveryMode received" +
        s" from members: ${persistentData.size()}")
    val itr = persistentData.iterator()
    val snapCon = SnappyContext()
    val snappyHiveExternalCatalog = HiveClientUtil
        .getOrCreateExternalCatalog(snapCon.sparkContext, snapCon.sparkContext.getConf)
    while (itr.hasNext) {
      val persistentViewObj = itr.next().asInstanceOf[PersistentStateInRecoveryMode]
      persistentObjectMemberMap += persistentViewObj.getMember -> persistentViewObj
      val regionItr = persistentViewObj.getAllRegionViews.iterator()
      while (regionItr.hasNext) {
        val persistentView = regionItr.next()
        val regionPath = persistentView.getRegionPath
        val set = regionViewSortedSet.get(regionPath)
        if (set.isDefined) {
          set.get += persistentView
        } else {
          var newset = mutable.SortedSet.empty[RecoveryModePersistentView]
          newset += persistentView
          regionViewSortedSet += regionPath -> newset
        }
      }
    }
    // identify which members catalog object to be used
    val hiveRegionViews = regionViewSortedSet.filterKeys(
      _.startsWith(SystemProperties.SNAPPY_HIVE_METASTORE_PATH))
    val hiveRegionToConsider =
      hiveRegionViews.keySet.toSeq.sortBy(hiveRegionViews.get(_).size).reverse.head
    val mostUptodateRegionView = regionViewSortedSet(hiveRegionToConsider).lastKey
    val memberToConsiderForHiveCatalog = mostUptodateRegionView.getMember
    val nonHiveRegionViews = regionViewSortedSet.filterKeys(
      !_.startsWith(SystemProperties.SNAPPY_HIVE_METASTORE_PATH))
    val regionToConsider =
      if (nonHiveRegionViews.nonEmpty) {
        nonHiveRegionViews.keySet.toSeq.sortBy(nonHiveRegionViews.get(_).size).reverse.head
      } else {
        logInfo("No relevant RecoveryModePersistentViews found.")
        throw new Exception("Cannot start empty cluster in Recovery Mode.")
      }
    val regionView = regionViewSortedSet(regionToConsider).lastKey
    val memberToConsider = regionView.getMember
    memberObject = persistentObjectMemberMap(memberToConsider)
    mostRecentMemberObject = persistentObjectMemberMap(memberToConsiderForHiveCatalog)
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
            catalogMetadataDetails.setCatalogTable(catTabObj), snapCon.sparkSession)._1
      }
    })
    RecoveryService.populateCatalog(catalogArr, snapCon.sparkContext)
    createSchemasMap(snappyHiveExternalCatalog)

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

  def getTables: Seq[CatalogTable] = {
    val snappyContext = SnappyContext()
    val snappyHiveExternalCatalog = HiveClientUtil
        .getOrCreateExternalCatalog(snappyContext.sparkContext, snappyContext.sparkContext.getConf)
    snappyHiveExternalCatalog.getAllTables().filter(!_.tableType.name.equalsIgnoreCase("view"))
  }

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
   * @param sc            Spark Context
   */

  def populateCatalog(catalogObjSeq: Seq[Any], sc: SparkContext): Unit = {
    val extCatalog = HiveClientUtil.getOrCreateExternalCatalog(sc, sc.getConf)
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

  case class DumpDataArgs(formatType: String, tables: Seq[String],
      outputDir: String, ignoreError: Boolean)
  val dataDumpArgs: mutable.MutableList[DumpDataArgs] = mutable.MutableList.empty

  /**
   * capture the arguments used by the procedure DUMP_DATA and cache them for later generating
   * helper scripts to load all this data back into new cluster
   *
   * @param formatType spark output format
   * @param tables comma separated qualified names of tables
   * @param outputDir base output path for one call of DUMP_DATA procedure
   * @param ignoreError whether to move on to next table in case of failure
   */
  def captureArguments(formatType: String, tables: Seq[String],
      outputDir: String, ignoreError: Boolean): Unit = {
    dataDumpArgs += new DumpDataArgs(formatType, tables, outputDir, ignoreError)
  }

}

object RegionDiskViewOrdering extends Ordering[RecoveryModePersistentView] {
  def compare(element1: RecoveryModePersistentView,
      element2: RecoveryModePersistentView): Int = {
    element2.compareTo(element1)
  }
}
