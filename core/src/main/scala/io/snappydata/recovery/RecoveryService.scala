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

import java.util.Map.Entry
import java.util.function.Consumer
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper
import com.gemstone.gemfire.internal.shared.SystemProperties
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector.ListResultCollectorValue
import com.pivotal.gemfirexd.internal.engine.distributed.message.PersistentStateInRecoveryMode
import com.pivotal.gemfirexd.internal.engine.distributed.message.PersistentStateInRecoveryMode.RecoveryModePersistentView
import com.pivotal.gemfirexd.internal.engine.sql.execute.RecoveredMetadataRequestMessage
import io.snappydata.Constant
import io.snappydata.sql.catalog.ConnectorExternalCatalog
import io.snappydata.thrift.{CatalogFunctionObject, CatalogMetadataDetails, CatalogSchemaObject, CatalogTableObject}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogFunction, CatalogTable}
import org.apache.spark.sql.collection.ToolsCallbackInit
import org.apache.spark.sql.hive.{HiveClientUtil, SnappyHiveExternalCatalog}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SnappyContext, SnappyParser, SnappySession}
import org.apache.spark.{Logging, SparkContext}

object RecoveryService extends Serializable with Logging {

  def getHiveDDLs: Array[String] = {
    null
  }

  def getAllDDLs(): mutable.Buffer[String] = {
    val buff: mutable.Buffer[String] = List.empty.toBuffer
    val snapCon = SnappyContext()
    val snappyHiveExternalCatalog = HiveClientUtil
        .getOrCreateExternalCatalog(snapCon.sparkContext, snapCon.sparkContext.getConf)
    val dbList = snappyHiveExternalCatalog.listDatabases("*").filter(dbName =>
      !(dbName.equalsIgnoreCase("SYS") || dbName.equalsIgnoreCase("DEFAULT")))
    val allFunctions = dbList.map(dbName => snappyHiveExternalCatalog.listFunctions(dbName, "*")
        .map(func => snappyHiveExternalCatalog.getFunction(dbName, func)))
    val allDatabases = dbList.map(snappyHiveExternalCatalog.getDatabase)
    allDatabases.foreach(db => {
      db.properties.get("orgSqlText") match {
        case Some(str) => if (!str.isEmpty) buff.append(str)
        case None => ""
      }
    })

    val allTables = snappyHiveExternalCatalog.getAllTables()
    allTables.foreach(table => {
      // covers create, alter, create view statements
      for (elem <- table.properties) {
        if (elem._1.contains("orgSqlText")) {
          buff append (elem._2)
        }
        else if (elem._1.contains("alt")) {
          buff append (elem._2)
        }
      }
      table.viewOriginalText match {
        case Some(str) => buff append (s"create view ${table.identifier} as $str")
        case None => ""
      }
    })

    val commands = ToolsCallbackInit.toolsCallback.getGlobalCmndsSet
    commands.forEach(new Consumer[Entry[String, String]] {
      override def accept(t: Entry[String, String]): Unit = {
        val alias = t.getKey
        val value = t.getValue
        val indexOf = value.indexOf('|')
        if (indexOf > 0) {
          // It is a package
          val pkg = value.substring(0, indexOf)
          buff.append(s"deploy package $alias $pkg")
        }
        else {
          // It is a jar
          val jars = value.split(',')
          val jarfiles = jars.map(f => {
            val lastIndexOf = f.lastIndexOf('/')
            val length = f.length
            if (lastIndexOf > 0) f.substring(lastIndexOf + 1, length)
            else {
              f
            }
          })
          buff.append(s"deploy jar $alias ${jarfiles.mkString(",")}")
        }
      }
    })
    buff
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
      bucketPath = s"/${PartitionedRegionHelper.PR_ROOT_REGION_NAME}/${PartitionedRegionHelper.getBucketName(tablePath, bucketId)}"
      // TODO remove replace used and handle it in a proper way
      // bucketPath = bucketPath.replace("/__PR/_B_", "/__PR/_B__")
    }
    for (entry <- regionViewSortedSet) {
      logInfo(s"1891: regionViewSortedSet[${entry._1}, ${entry._2}] and bucketPath = $bucketPath")
    }
    // check if the path exists else check path of column buffer.
    // also there could be no data in any.
    // check only row, only col, no data
    if (regionViewSortedSet.contains(bucketPath)) {
      Seq(regionViewSortedSet(bucketPath).lastKey.getExecutorHost)
    } else {
      logWarning("1891: getExecutorHost else ")
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
    logInfo(s"1891: cObj = ${cObj}")
    val tablePath = fqtn.replace(".", "/")
    import collection.JavaConversions._
    for ((s, i) <- mostRecentMemberObject.getPrToNumBuckets) {
      logInfo(s"1891: mostrecentmemberobject map ${s} -> ${i} and tablePath = ${tablePath}")
    }

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
    logInfo(s"1891: robj = ${robj == null} numbuckets = ${numBuckets}")
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
        table.storage.properties.get("SCHEMADDL.part.0") match {
          case Some(schemaStr) => {
            var schemaString = schemaStr
            val fqtn = table.identifier.database match {
              case Some(schName) => schName + "_" + table.identifier.table
              case None =>
                throw new Exception(s"Schema name not found for the table ${table.identifier.table}")
            }

            // todo: default value in the create statement is not supported - add support of it ...

            var schema = colParser.parseSQLOnly(schemaString, colParser.tableSchemaOpt.run())
                .map(StructType(_)) match {
              case Some(StructType(e)) => StructType(e)
              case None => null
            }
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
              if (stmt.contains("ADD COLUMN")) {
                // TODO replace ; at the end
                schemaString = schemaString
                    .patch(schemaString.length - 1,
                      ", " + stmt.substring(stmt.indexOf("ADD COLUMN ") + 11), 0).toUpperCase()
              } else if (stmt.contains("DROP COLUMN ")) {
                val col = stmt.substring(stmt.indexOf("DROP COLUMN ") + 12)
                // todo: use this str.split(",").map(s => s.trim.split("[ ]+")).filterNot(s => s(0).trim.equalsIgnoreCase("col2")).map(s=>s.mkString(" ")).mkString(",")
                val regex = s"([ ]*$col[ ]+[a-zA-Z][a-zA-Z0-9]+(\\([0-9]+(,?[0-9]+)?\\))?[ ]*,?)|(,[ ]*$col[ ]+[a-zA-Z][a-zA-Z0-9]+(\\([0-9]+(,?[0-9]+)?\\))?[ ]*)".r
                schemaString = regex.replaceAllIn(schemaString, "").toUpperCase()
              }
              // todo: default in alter fails with ParseException
              val schema = colParser.
                  parseSQLOnly(schemaString, colParser.tableSchemaOpt.run())
                  .map(StructType(_)) match {
                case Some(StructType(e)) => StructType(e)
                case None => null
              }
              schemaStructMap.put(s"${versionCnt}#${fqtn}", schema)
              val idArray: Array[Int] = new Array[Int](schema.fields.length)
              val prevSchema = getSchemaStructMap.getOrElse(s"${versionCnt - 1}#${fqtn}", null)
              val prevIdArray: Array[Int] = tableColumnIds.getOrElse(s"${versionCnt - 1}#${fqtn}", null)
              assert(prevSchema != null && prevIdArray != null)
              for (i <- 0 until schema.fields.length) {
                val prevId = prevSchema.fields.indexOf(schema.fields(i))
                idArray(i) = if (prevId == -1) {
                  idArray(i - 1) + 1
                } else {
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
    import collection.JavaConversions._
    for ((k, v) <- memberObject.getPrToNumBuckets) {
      logInfo(s"1891: PrToNumBuckets map values = ${k} -> ${v}")
    }
    logInfo(s"1891: PrToNumBuckets contains = ${memberContainsRegion} for member" + memberObject.getMember)
    if (memberContainsRegion) {
      (memberObject.getPrToNumBuckets.get(s"/${schemaName}/${tableName}"), false)
    } else {
      logWarning(s"1891: num of buckets not found for /${schemaName}/${tableName}")
      if (memberObject.getreplicatedRegions().contains(s"/${schemaName}/${tableName}")) {
        logInfo("1891: table is replicated ")
        (1, true)
      } else (-1, false)
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
      logInfo(s"1891: persistentObjectMemberMap = ${k} ${v}")
    }
    mostRecentMemberObject = persistentObjectMemberMap(memberToConsiderForHiveCatalog)
    val otherExtractedDDLs = mostRecentMemberObject.getOtherDDLs
    println(s"Other extracted ddls are = $otherExtractedDDLs")
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
          logInfo(s"1891: RecoveryService catalogthriftObj = $catTabObj and numbuckets = ${catTabObj.getNumBuckets}")
          val ctobj = ConnectorExternalCatalog.convertToCatalogTable(
            catalogMetadataDetails.setCatalogTable(catTabObj), snapCon.sparkSession)._1
          val str = ctobj.properties.mkString(":")
          logInfo(s"1891: RecoveryService catalogTableObj = $ctobj and properties = ${str}")
          ctobj
        }
      }
    })

    RecoveryService.populateCatalog(catalogArr, snapCon.sparkContext)
    val dbList = snappyHiveExternalCatalog.listDatabases("*")
    val allFunctions = dbList.map(dbName => snappyHiveExternalCatalog.listFunctions(dbName, "*")
        .map(func => snappyHiveExternalCatalog.getFunction(dbName, func)))
    val allDatabases = dbList.map(snappyHiveExternalCatalog.getDatabase)

    logInfo("PP:RecoveryService: Catalog contents in recovery mode:\nTables\n"
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
