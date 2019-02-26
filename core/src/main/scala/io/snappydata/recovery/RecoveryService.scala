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

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper
import com.gemstone.gemfire.internal.shared.SystemProperties
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector.ListResultCollectorValue
import com.pivotal.gemfirexd.internal.engine.distributed.message.PersistentStateInRecoveryMode
import com.pivotal.gemfirexd.internal.engine.distributed.message.PersistentStateInRecoveryMode.RecoveryModePersistentView
import com.pivotal.gemfirexd.internal.engine.sql.execute.RecoveredMetadataRequestMessage
import io.snappydata.sql.catalog.{CatalogObjectType, ConnectorExternalCatalog}
import io.snappydata.thrift.{CatalogFunctionObject, CatalogMetadataDetails, CatalogSchemaObject, CatalogTableObject}
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.Constant
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogFunction, CatalogTable}
import org.apache.spark.sql.hive.HiveClientUtil

object RecoveryService extends Logging {

  def getHiveDDLs: Array[String] = {
    null
  }

  /* This should dump the real ddls into the o/p file */
  def dumpAllDDLs(path: URI): Unit = {

  }

  /* fqtn and bucket number for PR r Column table, -1 indicates replicated row table */
  def getExecutorHost(tableName: String, bucketId: Int = -1): Seq[String] = {
    // Expecting table in the format fqtn i.e. schemaname.tablename
    val tablePath = tableName.replace(".", "/")
    var bucketPath = tablePath
    if (bucketId >= 0) {
      // bucketPath = PartitionedRegionHelper.getBucketFullPath(tablePath, bucketId)
      bucketPath = s"/${PartitionedRegionHelper.PR_ROOT_REGION_NAME}/${PartitionedRegionHelper.getBucketName(tablePath, bucketId)}"
    }
    // TODO remove replace used and handle it in a proper way
    bucketPath = bucketPath.replace("/__PR/_B_", "/__PR/_B__")
    for (entry <- regionViewSortedSet) {
      logInfo(s"1891: regionViewSortedSet[${entry._1}, ${entry._2}] and bucketPath = $bucketPath" )
    }
    Seq(regionViewSortedSet(bucketPath).lastKey.getExecutorHost)
  }

  /* Table type, PR or replicated, DStore name, numBuckets */
  def getTableDiskInfo(fqtn: String):
  Tuple4[String, Boolean, String, Int] = {
    val parts = fqtn.split("\\.")
    val schema = parts(0)
    val table = parts(1)
    val cObject = mostRecentMemberObject.getCatalogObjects.toArray()
// most effective way ??
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
    val cObj = cObjArr(0).asInstanceOf[CatalogTableObject]
    logInfo(s"1891: cObj = ${cObj}")
    val tablePath = fqtn.replace(".", "/")
    import collection.JavaConversions._
    for((s, i) <- mostRecentMemberObject.getPrToNumBuckets){
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
      logInfo(s"1891: cVARD persistentViewObj${persistentViewObj}")

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
    logInfo(" Populated catalog")

    val dbList = snappyHiveExternalCatalog.listDatabases("*")
    val allFunctions = dbList.map(dbName => snappyHiveExternalCatalog.listFunctions(dbName, "*")
        .map(func => snappyHiveExternalCatalog.getFunction(dbName, func)))
    val allDatabases = dbList.map(snappyHiveExternalCatalog.getDatabase)

    logInfo("PP:RecoveryService: Catalog contents in recovery mode:\nTables\n"
        + snappyHiveExternalCatalog.getAllTables().toString() + "\nDatabases\n"
        + allDatabases.toString() + "\nFunctions\n" + allFunctions.toString())
  }

  def getProvider(tableName: String): String = {
    logInfo(s"RecoveryService.getProvider called with tablename $tableName")
    val res = mostRecentMemberObject.getCatalogObjects.asScala.filter( x => {
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
   * @param catalogObjSeq   Sequence of catalog objects to be inserted in the catalog
   * @param sc              Spark Context
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
