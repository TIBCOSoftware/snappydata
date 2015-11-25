package org.apache.spark.sql.store.impl


import java.net.ConnectException
import java.sql.{SQLException, DriverManager, Connection}
import java.util.Properties

import com.gemstone.gemfire.internal.SocketCreator
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.{AbstractRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql.collection.{ExecutorLocalShellPartition, MultiExecutorLocalPartition, UUIDRegionKey}
import org.apache.spark.sql.columnar.{ExternalStoreUtils, CachedBatch, ConnectionType}

import org.apache.spark.sql.collection.{MultiExecutorLocalPartition, UUIDRegionKey}
import org.apache.spark.sql.store.util.StoreUtils
import org.apache.spark.sql.store.{CachedBatchIteratorOnRS, JDBCSourceAsStore}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Partition, SparkContext, TaskContext}
import io.snappydata.{Constant, Utils}

import scala.util.Random

/**
 * Columnar Store implementation for GemFireXD.
 *
 */
final class JDBCSourceAsColumnarStore(_url: String,
    _driver: String,
    _poolProps: Map[String, String],
    _connProps: Properties,
    _hikariCP: Boolean,
    val blockMap: Map[InternalDistributedMember, BlockManagerId])
    extends JDBCSourceAsStore(_url, _driver, _poolProps, _connProps, _hikariCP) {

  override def getCachedBatchRDD(tableName: String, requiredColumns: Array[String],
      uuidList: ArrayBuffer[RDD[UUIDRegionKey]],
      sparkContext: SparkContext): RDD[CachedBatch] = {
    connectionType match {
      case ConnectionType.Embedded =>
        new ColumnarStorePartitionedRDD[CachedBatch](sparkContext,
          tableName, requiredColumns, this)
      case _ =>
        if (ExternalStoreUtils.isExternalShellMode(sparkContext))
          new ShellPartitionedRDD[CachedBatch](sparkContext,
            getConnection(tableName).getSchema, tableName, requiredColumns, this)
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
                                tableName: String): UUIDRegionKey = {
    val connection: java.sql.Connection = getConnection(tableName)
    try {
      val uuid = connectionType match {

        case ConnectionType.Embedded =>
          val resolvedName = StoreUtils.lookupName(tableName, connection.getSchema)
          val region = Misc.getRegionForTable(resolvedName, true)
          region.asInstanceOf[AbstractRegion] match {
            case pr: PartitionedRegion =>
              val primaryBuckets = pr.getDataStore.getAllLocalPrimaryBucketIds
                .toArray(new Array[Integer](0))
              genUUIDRegionKey(rand.nextInt(primaryBuckets.size))
            case _ =>
              genUUIDRegionKey()
          }

        case _ =>
          genUUIDRegionKey(rand.nextInt(Integer.MAX_VALUE))
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


}


class ColumnarStorePartitionedRDD[T: ClassTag](@transient _sc: SparkContext,
    tableName: String,
    requiredColumns: Array[String], store: JDBCSourceAsColumnarStore)
    extends RDD[CachedBatch](_sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[CachedBatch] = {
    store.tryExecute(tableName, {
      case conn =>
        val resolvedName = StoreUtils.lookupName(tableName, conn.getSchema)
        val par = split.index
        val ps1 = conn.prepareStatement(s"call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION('$resolvedName', $par)")
        ps1.execute()
        val ps = conn.prepareStatement(s"select stats, " +
            requiredColumns.mkString(", ") + s" from $tableName")

        val rs = ps.executeQuery()

        new CachedBatchIteratorOnRS(conn, store.connectionType, requiredColumns, ps, rs)
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

class ShellPartitionedRDD[T: ClassTag](@transient _sc: SparkContext, schema: String,
                                       tableName: String, requiredColumns: Array[String],
                                       store: JDBCSourceAsColumnarStore)
  extends RDD[CachedBatch](_sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[CachedBatch] = {
    DriverRegistry.register(Constant.JDBC_CLIENT_DRIVER )
    val resolvedName = StoreUtils.lookupName(tableName, schema)

    //TODO use connection Pool
    val conn = getConnection(ArrayBuffer(split.asInstanceOf[ExecutorLocalShellPartition].hostList:_*))

    conn.setTransactionIsolation(Connection.TRANSACTION_NONE)

    val par = split.index

    val statement = conn.createStatement();
    val query = s"select stats ," + requiredColumns.mkString(" ", ",", " ") + " from " + resolvedName
    statement.execute(s"call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION('$resolvedName', $par)");
    val rs = statement.executeQuery(query)
    new CachedBatchIteratorOnRS(conn, store.connectionType, requiredColumns, statement, rs)

  }


  def getConnection(hostList: ArrayBuffer[(String, String)]): Connection = {
    val localhost = SocketCreator.getLocalHost
    var index = -1
      if (index < 0)  index = hostList.indexWhere(e => e._2.contains(localhost.getHostAddress))
      if (index < 0 ) index = hostList.indexWhere(e => e._2.contains(localhost.getCanonicalHostName))
      if (index < 0) index = Random.nextInt(hostList.size)

    try {
      DriverManager.getConnection(hostList(index)._2)
    } catch {
      case sqlException: SQLException =>
        if (hostList.size == 1)
          throw sqlException
        else {
          hostList.remove(index)
          getConnection(hostList)
        }
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    //_sc.getConf.getExecutorEnv.get
    split.asInstanceOf[ExecutorLocalShellPartition]
      .hostList.map(tuple => tuple._1.asInstanceOf[String]).toSeq
  }

  override protected def getPartitions: Array[Partition] = {
    val resolvedName = StoreUtils.lookupName(tableName, schema)
    val bucketToServerList = getBucketToServerMapping(resolvedName)
    val numPartitions = bucketToServerList.size
    val partitions = new Array[Partition](numPartitions)
    for (p <- 0 until numPartitions) {
      partitions(p) = new ExecutorLocalShellPartition(p, bucketToServerList(p) )
    }
    partitions
  }


  private def getBucketToServerMapping(resolvedName: String): Array[Array[(String, String)]] = {
    //todo - replicated regions needs to be handled or not required????
    val urlPrefix = "jdbc:" + Constant.JDBC_URL_PREFIX
    val region: PartitionedRegion = Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]
    val bidToAdvisorMap = region.getRegionAdvisor.getAllBucketAdvisorsHostedAndProxies
    val distributedMembersToNetServerMap = GemFireXDUtils.getGfxdAdvisor.
      getAllDRDAServersAndCorrespondingMemberMapping
    val serverToBucketMapping = {
      for (bid <- bidToAdvisorMap.keySet.toArray
           if bidToAdvisorMap.get(bid).getProxyBucketRegion.getBucketOwners.size() > 0)
        yield {
          val bOwners = bidToAdvisorMap.get(bid).getProxyBucketRegion
            .getBucketOwners.toArray
            .map(owner => owner.asInstanceOf[InternalDistributedMember])
          val serverPerBucket = {
            for (bOwner: InternalDistributedMember <- bOwners)
              yield {
                val netServer: String = distributedMembersToNetServerMap.get(bOwner)
                val hostNameWithAddress = netServer.substring(0,netServer.indexOf("[")).split("/")
                val host =  if (hostNameWithAddress(0) == "" || hostNameWithAddress(0) == null )
                  hostNameWithAddress(1)
                else hostNameWithAddress (0)
                val clientPort = netServer.substring(netServer.indexOf("[") + 1, netServer.indexOf("]"))
                Tuple2(bOwner.getIpAddress.getHostAddress, s"$urlPrefix$host:$clientPort")
              }
          }
          serverPerBucket
        }
    }
    serverToBucketMapping
  }

}
