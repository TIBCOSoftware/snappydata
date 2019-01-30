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
      bucketPath = PartitionedRegionHelper.getBucketFullPath(tablePath, bucketId)
    }
    Seq(regionViewSortedSet(bucketPath).lastKey.getExecutorHost)
  }

  /* Table type, PR or replicated, DStore name, numBuckets */
  def getTableDiskInfo(fqtn: String):
  Tuple4[String, Boolean, String, Int] = {
    val parts = fqtn.split(".")
    val schema = parts(0)
    val table = parts(1)
    val cObj = mostRecentMemberObject.getCatalogObjects.asScala.find(c => {
      c.schemaName == schema && c.tableName == table
    })
    cObj.get.provider
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
    (cObj.get.provider, numBuckets != null, robj.get.getDiskStoreName, numBuckets)
  }

  val regionViewSortedSet: mutable.Map[String,
      mutable.SortedSet[RecoveryModePersistentView]] = mutable.Map.empty

  val persistentObjectMemberMap: mutable.Map[
      InternalDistributedMember, PersistentStateInRecoveryMode ] = mutable.Map.empty

  var mostRecentMemberObject: PersistentStateInRecoveryMode = null;


  def collectViewsAndRecoverDDLs(): Unit = {
    // Send a message to all the servers and locators to send back their
    // respective persistent state information.
    val collector = new GfxdListResultCollector(null, true)
    val msg = new RecoveredMetadataRequestMessage(collector)
    msg.executeFunction()
    val persistentData = collector.getResult
    val itr = persistentData.iterator()

    while (itr.hasNext) {
      val persistentViewObj = itr.next().asInstanceOf[
          ListResultCollectorValue].resultOfSingleExecution.asInstanceOf[
          PersistentStateInRecoveryMode]
      logInfo(Misc.getMemStore.getMyVMKind.toString +
          s"\nPP: persistentViewObj = $persistentViewObj " + "0*0\n")
//      TODO: need to handle null pointer exception here - at following line??
      persistentObjectMemberMap += persistentViewObj.getMember -> persistentViewObj
      val regionItr = persistentViewObj.getAllRegionViews.iterator()
      while(regionItr.hasNext) {
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
    for((k, v) <- regionViewSortedSet) {
      println(s"region = $k and set = $v")
    }

    // identify which members catalog object to be used
    val hiveRegionViews = regionViewSortedSet.filterKeys(
      _.startsWith(SystemProperties.SNAPPY_HIVE_METASTORE_PATH))

    val hiveRegionToConsider =
      hiveRegionViews.keySet.toSeq.sortBy(hiveRegionViews.get(_).size).reverse.head
    println(s"Hive region to consider = $hiveRegionToConsider")

    val mostUptodateRegionView = regionViewSortedSet(hiveRegionToConsider).lastKey

    val memberToConsiderForHiveCatalog = mostUptodateRegionView.getMember

    println(s"For Hive memberToConsiderForHiveCatalog = $memberToConsiderForHiveCatalog")

    mostRecentMemberObject = persistentObjectMemberMap(memberToConsiderForHiveCatalog)
    val otherExtractedDDLs = mostRecentMemberObject.getOtherDDLs

    println(s"Other extracted ddls are = $otherExtractedDDLs")

    val catalogObjects = mostRecentMemberObject.getCatalogObjects

    logInfo(s"Catalog objects = $catalogObjects")


    val snapCon = SnappyContext()
    import scala.collection.JavaConverters._

    val catalogArr = catalogObjects.asScala.map(catObj => {
    val catalogMetadataDetails = new CatalogMetadataDetails()
    catObj match {
//      case catFunObj: CatalogFunctionObject => {
//        ConnectorExternalCatalog.convertToCatalogFunction(catFunObj)
//      }
//      case catDBObj: CatalogSchemaObject => {
//        ConnectorExternalCatalog.convertToCatalogDatabase(catDBObj)
//      }
      case catTabObj: CatalogTableObject => {
        ConnectorExternalCatalog.convertToCatalogTable(
          catalogMetadataDetails.setCatalogTable(catTabObj), snapCon.sparkSession )._1
      }
    }
    })

    logInfo(Misc.getMemStore.getMyVMKind.toString +
        "  ===  catalog table array :\nlength :" + catalogArr.length +
        "\n the array: " + catalogArr)

    // TODO: for now populating a dummy app schema - catalog object - remove it later
    val appCatDB = new CatalogDatabase("APP", "User APP schema",
      "file:/home/ppatil/git_snappy/snappydata/build-artifacts/scala-2.11" +
          "/snappy/work/localhost-lead-1/spark-warehouse/APP.db", Map.empty)

    if(!(Misc.getMemStore.getMyVMKind.isLocator || Misc.getMemStore.getMyVMKind.isStore)) {
      RecoveryService.populateCatalog(Seq(appCatDB), snapCon.sparkContext)
    }
    RecoveryService.populateCatalog(catalogArr, snapCon.sparkContext)
    logInfo(" populated catalog")

    val snappycat = HiveClientUtil
        .getOrCreateExternalCatalog(snapCon.sparkContext, snapCon.sparkContext.getConf)

    logInfo("Tables in catalog - in recovery mode" + snappycat.getAllTables().toString())


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
        extCatalog.createDatabase(catDB, ignoreIfExists = false)
        logInfo(s"Inserting catalog database: $catDB in the catalog.")
      case catFunc: CatalogFunction =>
        extCatalog.createFunction(catFunc.identifier.database
            .getOrElse(Constant.DEFAULT_SCHEMA), catFunc)
        logInfo(s"Inserting catalog function: $catFunc in the catalog.")
      case catTab: CatalogTable =>
        extCatalog.createTable(catTab, ignoreIfExists = false)
        logInfo(s"Inserting catalog table: $catTab in the catalog.")
    }
  }

}

object RegionDiskViewOrdering extends Ordering[RecoveryModePersistentView] {
  def compare(element1: RecoveryModePersistentView,
    element2: RecoveryModePersistentView): Int = {
    element2.compareTo(element1)
  }
}
