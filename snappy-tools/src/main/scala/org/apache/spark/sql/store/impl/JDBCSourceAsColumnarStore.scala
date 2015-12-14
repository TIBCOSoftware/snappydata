package org.apache.spark.sql.store.impl

import java.sql.{Connection, SQLException}
import java.util.{Properties, UUID}

import com.gemstone.gemfire.cache.Region
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import com.gemstone.gemfire.internal.SocketCreator
import com.gemstone.gemfire.internal.cache.{AbstractRegion, DistributedRegion, NoDataStoreAvailableException, PartitionedRegion}
import com.gemstone.gemfire.internal.i18n.LocalizedStrings
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.jdbc.ClientAttribute
import io.snappydata.Constant
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql.collection.{ExecutorLocalShellPartition, MultiExecutorLocalPartition, UUIDRegionKey, Utils}
import org.apache.spark.sql.columnar.{CachedBatch, ConnectionType, ExternalStoreUtils}
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.row.GemFireXDClientDialect
import org.apache.spark.sql.store.{JDBCSourceAsStore, CachedBatchIteratorOnRS, StoreUtils}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random
/**
 * Column Store implementation for GemFireXD.
 */
final class JDBCSourceAsColumnarStore(_url: String,
    _driver: String,
    _poolProps: Map[String, String],
    _connProps: Properties,
    _hikariCP: Boolean,
    val blockMap: Map[InternalDistributedMember, BlockManagerId] = null)
    extends JDBCSourceAsStore(_url, _driver, _poolProps, _connProps, _hikariCP) {

  override def getCachedBatchRDD(tableName: String, requiredColumns: Array[String],
      uuidList: ArrayBuffer[RDD[UUIDRegionKey]],
      sparkContext: SparkContext): RDD[CachedBatch] = {
    connectionType match {
      case ConnectionType.Embedded =>
        new ColumnarStorePartitionedRDD[CachedBatch](sparkContext,
          tableName, requiredColumns, this)
      case _ =>
        if (ExternalStoreUtils.isExternalShellMode(sparkContext)) {
          // remove the url property from poolProps since that will be
          // partition-specific
          val poolProps = this.poolProps - (if (hikariCP) "jdbcUrl" else "url")

          new ShellPartitionedRDD[CachedBatch](sparkContext,
            getConnection(tableName).getSchema, tableName, requiredColumns,
            poolProps, connProps, hikariCP , url)
          }
         else {
          var rddList = new ArrayBuffer[RDD[CachedBatch]]()
          uuidList.foreach(x => {
            val y = x.mapPartitions { uuidItr =>
              getCachedBatchIterator(tableName, requiredColumns, uuidItr)
            }
            rddList += y
          })
          new UnionRDD[CachedBatch](sparkContext, rddList)
        }
    }
  }

  override def storeCachedBatch(batch: CachedBatch,
                                tableName: String, maxPartitions: Int): UUIDRegionKey = {
    val connection: java.sql.Connection = getConnection(tableName)
    try {
      val uuid = connectionType match {
        case ConnectionType.Embedded =>
          val resolvedName = StoreUtils.lookupName(tableName, connection.getSchema)
          val region = Misc.getRegionForTable(resolvedName, true)
          region.asInstanceOf[Region[_, _]] match {
            case pr: PartitionedRegion =>
              val primaryBucketIds = pr.getDataStore.
                  getAllLocalPrimaryBucketIdArray
              genUUIDRegionKey(primaryBucketIds.getQuick(
                rand.nextInt(primaryBucketIds.size())))
            case _ =>
              genUUIDRegionKey()
          }

        case _ =>
          genUUIDRegionKey(rand.nextInt(maxPartitions))
      }

      val rowInsertStr = getRowInsertStr(tableName, batch.buffers.length)
      val stmt = connection.prepareStatement(rowInsertStr)
      stmt.setString(1, uuid.getUUID.toString)
      stmt.setInt(2, uuid.getBucketId)
      stmt.setBytes(3, serializer.newInstance().serialize(batch.stats).array())
      var columnIndex = 4
      batch.buffers.foreach(buffer => {
        stmt.setBytes(columnIndex, buffer)
        columnIndex += 1
      })
      stmt.executeUpdate()
      stmt.close()
      uuid
    } finally {
      connection.close()
    }
  }

  override def storeCachedBatch(batch: CachedBatch, batchID: UUID, bucketID: Int,
      tableName: String): UUIDRegionKey = {
    val connection: java.sql.Connection = getConnection(tableName)
    try {
      val uuid = connectionType match {
        case ConnectionType.Embedded =>
          val resolvedName = StoreUtils.lookupName(tableName, connection.getSchema)
          val region = Misc.getRegionForTable(resolvedName, true)
          region.asInstanceOf[AbstractRegion] match {
            case pr: PartitionedRegion =>
              genUUIDRegionKey(bucketID, batchID)
            case _ =>
              genUUIDRegionKey()
          }

        case _ => genUUIDRegionKey()
      }

      val rowInsertStr = getRowInsertStr(tableName, batch.buffers.length)
      val stmt = connection.prepareStatement(rowInsertStr)
      stmt.setString(1, uuid.getUUID.toString)
      stmt.setInt(2, uuid.getBucketId)
      stmt.setInt(3, batch.numRows)
      stmt.setBytes(4, serializer.newInstance().serialize(batch.stats).array())
      var columnIndex = 5
      batch.buffers.foreach(buffer => {
        stmt.setBytes(columnIndex, buffer)
        columnIndex += 1
      })
      stmt.executeUpdate()
      stmt.close()
      uuid
    } finally {
      connection.close()
    }
  }
}

class ColumnarStorePartitionedRDD[T: ClassTag](@transient _sc: SparkContext,
    tableName: String,
    requiredColumns: Array[String], store: JDBCSourceAsColumnarStore)
    extends RDD[CachedBatch](_sc, Nil) with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[CachedBatch] = {
    store.tryExecute(tableName, {
      case conn =>
        val resolvedName = StoreUtils.lookupName(tableName, conn.getSchema)
        val par = split.index
        val ps1 = conn.prepareStatement(
          "call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION(?, ?)")
        ps1.setString(1, resolvedName)
        ps1.setInt(2, par)
        ps1.execute()

        val ps = conn.prepareStatement("select " + requiredColumns.mkString(
          ", ") + ", numRows, stats from " + tableName)

        val rs = ps.executeQuery()
        ps1.close()
        new CachedBatchIteratorOnRS(conn, requiredColumns, ps, rs)
    }, closeOnSuccess = false)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[MultiExecutorLocalPartition].hostExecutorIds
  }

  override protected def getPartitions: Array[Partition] = {
    store.tryExecute(tableName, {
      case conn =>
        val tableSchema = conn.getSchema
        StoreUtils.getPartitionsPartitionedTable(_sc, tableName, tableSchema, store.blockMap)
    })
  }
}

class ShellPartitionedRDD[T: ClassTag](@transient _sc: SparkContext,
    schema: String, tableName: String, requiredColumns: Array[String],
    poolProps: Map[String, String], connProps: Properties, hikariCP: Boolean , locatorUrl:String)
    extends RDD[CachedBatch](_sc, Nil) {

  override def compute(split: Partition,
                       context: TaskContext): Iterator[CachedBatch] = {
    DriverRegistry.register(Constant.JDBC_CLIENT_DRIVER)
    val par = split.index
    val resolvedName = StoreUtils.lookupName(tableName, schema)
    val urlsOfNetServerHost = split.asInstanceOf[ExecutorLocalShellPartition].hostList
    val useLocatorURL = useLocatorUrl(urlsOfNetServerHost)

    val conn = getConnection(urlsOfNetServerHost, useLocatorURL)
    val query = "select " + requiredColumns.mkString(", ") +
        ", numRows, stats from " + resolvedName + (if (useLocatorURL) s" where bucketId = $par"
    else " ")

    val statement = conn.createStatement()
    if (!useLocatorURL)
      statement.execute(s"call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION('$resolvedName', $par)")
    val rs = statement.executeQuery(query)

    new CachedBatchIteratorOnRS(conn, requiredColumns, statement, rs)
  }

  def getConnection(hostList: ArrayBuffer[(String, String)] , connectToLocator:Boolean): Connection = {
    val localhost = SocketCreator.getLocalHost
    var index = -1

    // setup pool properties
    val maxPoolSize = String.valueOf(math.max(
      32, Runtime.getRuntime.availableProcessors() * 2))

    val jdbcUrl = if (connectToLocator) {
      locatorUrl
    } else {
      if (index < 0) index = hostList.indexWhere(_._1.contains(localhost.getHostAddress))
      if (index < 0) index = Random.nextInt(hostList.size)
      hostList(index)._2
    }
    val props = if (hikariCP) {
      poolProps + ("jdbcUrl" -> jdbcUrl) + ("maximumPoolSize" -> maxPoolSize)
    } else {
      poolProps + ("url" -> jdbcUrl) + ("maxActive" -> maxPoolSize)
    }
    try {
      // use jdbcUrl as the key since a unique pool is required for each server
      ConnectionPool.getPoolConnection(jdbcUrl, None,
        GemFireXDClientDialect, props, connProps, hikariCP)
    } catch {
      case sqlException: SQLException =>
        if (hostList.size == 1 || connectToLocator)
          throw sqlException
        else {
          hostList.remove(index)
          getConnection(hostList , connectToLocator)
        }
    }
  }

  def useLocatorUrl(hostList: ArrayBuffer[(String, String)]): Boolean = {
    hostList.size == 0
  }



  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[ExecutorLocalShellPartition]
        .hostList.map(_._1.asInstanceOf[String]).toSeq
  }

  override protected def getPartitions: Array[Partition] = {
    val resolvedName = StoreUtils.lookupName(tableName, schema)
    val bucketToServerList = getBucketToServerMapping(resolvedName)
    val numPartitions = bucketToServerList.length
    val partitions = new Array[Partition](numPartitions)
    for (p <- 0 until numPartitions) {
      partitions(p) = new ExecutorLocalShellPartition(p, bucketToServerList(p))
    }
    partitions
  }

  private def fillNetUrlsForServer(node: InternalDistributedMember,
      membersToNetServers: java.util.Map[InternalDistributedMember, String],
      urlPrefix: String, urlSuffix: String,
      netUrls: ArrayBuffer[(String, String)]): Unit = {
    val netServers: String = membersToNetServers.get(node)
    if (netServers != null && !netServers.isEmpty) {
      // check the rare case of multiple network servers
      if (netServers.indexOf(',') > 0) {
        for (netServer <- netServers.split(",")) {
          netUrls += node.getIpAddress.getHostAddress ->
              (urlPrefix + Utils.getClientHostPort(netServer) + urlSuffix)
        }
      } else {
        netUrls += node.getIpAddress.getHostAddress ->
            (urlPrefix + Utils.getClientHostPort(netServers) + urlSuffix)
      }
    }
  }

  private def getBucketToServerMapping(
      resolvedName: String): Array[ArrayBuffer[(String, String)]] = {
    val urlPrefix = "jdbc:" + Constant.JDBC_URL_PREFIX
    // no query routing or load-balancing
    val urlSuffix = "/" + ClientAttribute.ROUTE_QUERY + "=false;" +
        ClientAttribute.LOAD_BALANCE + "=false"
    val membersToNetServers = GemFireXDUtils.getGfxdAdvisor.
        getAllDRDAServersAndCorrespondingMemberMapping
    Misc.getRegionForTable(resolvedName, true).asInstanceOf[Region[_, _]] match {
      case pr: PartitionedRegion =>
        val bidToAdvisorMap = pr.getRegionAdvisor.
            getAllBucketAdvisorsHostedAndProxies
        val numBuckets = bidToAdvisorMap.size()
        val allNetUrls = new Array[ArrayBuffer[(String, String)]](numBuckets)
        for (bid <- 0 until numBuckets) {
          val pbr = bidToAdvisorMap.get(bid).getProxyBucketRegion
          // throws PartitionOfflineException if appropriate
          pbr.checkBucketRedundancyBeforeGrab(null, false)
          val bOwners = pbr.getBucketOwners
          val netUrls = ArrayBuffer.empty[(String, String)]
          bOwners.asScala.foreach(fillNetUrlsForServer(_,
            membersToNetServers, urlPrefix, urlSuffix, netUrls))
          allNetUrls(bid) = netUrls
        }
        allNetUrls

      case dr: DistributedRegion =>
        val owners = dr.getDistributionAdvisor.adviseInitializedReplicates()
        val netUrls = ArrayBuffer.empty[(String, String)]
        owners.asScala.foreach(fillNetUrlsForServer(_, membersToNetServers,
          urlPrefix, urlSuffix, netUrls))
        Array(netUrls)

      case r => sys.error("unexpected region with dataPolicy=" +
          s"${r.getAttributes.getDataPolicy} attributes: ${r.getAttributes}")
    }
  }
}
