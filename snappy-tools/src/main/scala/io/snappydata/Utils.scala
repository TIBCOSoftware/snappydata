package io.snappydata

import java.sql.{ResultSet, Statement, SQLException, Connection}
import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import com.gemstone.gemfire.SystemFailure
import com.gemstone.gemfire.cache.Region
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.LogWriterImpl.GemFireThreadGroup
import com.gemstone.gemfire.internal.SocketCreator
import com.gemstone.gemfire.internal.cache.{DistributedRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.jdbc.ClientAttribute
import org.apache.spark.Partition
import org.apache.spark.sql.collection.{ExecutorLocalShellPartition}
import org.apache.spark.sql.columnar.ConnectionProperties
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.row.GemFireXDClientDialect
import org.apache.spark.sql.store.{ExternalStore, StoreUtils}
import scala.collection.JavaConverters._
/**
 * Created by soubhikc on 20/10/15.
 */
object Utils {
  val LocatorURLPattern = Pattern.compile("(.+:[0-9]+)|(.+\\[[0-9]+\\])");

  val SnappyDataThreadGroup = new GemFireThreadGroup("SnappyData Thread Group") {
    override def uncaughtException(t: Thread, e: Throwable) {
      if (e.isInstanceOf[Error] && SystemFailure.isJVMFailureError(e.asInstanceOf[Error])) {
        SystemFailure.setFailure(e.asInstanceOf[Error])
      }
      Thread.dumpStack
    }
  }

  def getFields(o: Any): Map[String, Any] = {
    val fieldsAsPairs = for (field <- o.getClass.getDeclaredFields) yield {
      field.setAccessible(true)
      (field.getName, field.get(o))
    }
    Map(fieldsAsPairs: _*)
  }
}

object SparkShellRDDHelper {

  var useLocatorURL: Boolean = false

  def getSQLStatement(resolvedTableName: String, requiredColumns: Array[String], partitionId: Int)
  : String = {
    val whereClause = if (useLocatorURL) s" where bucketId = $partitionId" else ""
    "select " + requiredColumns.mkString(", ") +
        ", numRows, stats from " + resolvedTableName + whereClause
  }

  def executeQuery(conn: Connection, tableName: String,
      split: Partition, query: String): (Statement, ResultSet) = {
    DriverRegistry.register(Constant.JDBC_CLIENT_DRIVER)
    val resolvedName = StoreUtils.lookupName(tableName, conn.getSchema)
    val par = split.index
    val statement = conn.createStatement()

    if (!useLocatorURL)
      statement.execute(s"call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION('$resolvedName', $par)")

    val rs = statement.executeQuery(query)
    (statement, rs)
  }


  def getConnection(connectionProperties: ConnectionProperties, split: Partition): Connection = {
    val par = split.index
    val urlsOfNetServerHost = split.asInstanceOf[ExecutorLocalShellPartition].hostList
    useLocatorURL = useLocatorUrl(urlsOfNetServerHost)
    createConnection(connectionProperties, urlsOfNetServerHost)
  }

  def getPartitions(tableName: String, store: ExternalStore): Array[Partition] = {
    useLocatorURL=false;
    store.tryExecute(tableName, {
      case conn =>
        val resolvedName = StoreUtils.lookupName(tableName, conn.getSchema)
        val bucketToServerList = getBucketToServerMapping(resolvedName)
        val numPartitions = bucketToServerList.length
        val partitions = new Array[Partition](numPartitions)
        for (p <- 0 until numPartitions) {
          partitions(p) = new ExecutorLocalShellPartition(p, bucketToServerList(p))
        }
        partitions
    })
  }

  def createConnection(connProps: ConnectionProperties, hostList: ArrayBuffer[(String, String)])
  : Connection = {
    val localhost = SocketCreator.getLocalHost
    var index = -1
    // setup pool properties
    val maxPoolSize = String.valueOf(math.max(
      32, Runtime.getRuntime.availableProcessors() * 2))

    val jdbcUrl = if (useLocatorURL) {
      connProps.url
    } else {
      if (index < 0) index = hostList.indexWhere(_._1.contains(localhost.getHostAddress))
      if (index < 0) index = Random.nextInt(hostList.size)
      hostList(index)._2
    }

    val props = if (connProps.hikariCP) {
      connProps.poolProps + ("jdbcUrl" -> jdbcUrl) + ("maximumPoolSize" -> maxPoolSize)
    } else {
      connProps.poolProps + ("url" -> jdbcUrl) + ("maxActive" -> maxPoolSize)
    }
    try {
      // use jdbcUrl as the key since a unique pool is required for each server
      ConnectionPool.getPoolConnection(jdbcUrl, None,
        GemFireXDClientDialect, props, connProps.connProps, connProps.hikariCP)
    } catch {
      case sqlException: SQLException =>
        if (hostList.size == 1 || useLocatorURL)
          throw sqlException
        else {
          hostList.remove(index)
          createConnection(connProps, hostList)
        }
    }
  }

  private def useLocatorUrl(hostList: ArrayBuffer[(String, String)]): Boolean = {
    hostList.size == 0
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
              (urlPrefix + org.apache.spark.sql.collection.Utils.getClientHostPort(netServer) + urlSuffix)
        }
      } else {
        netUrls += node.getIpAddress.getHostAddress ->
            (urlPrefix + org.apache.spark.sql.collection.Utils.getClientHostPort(netServers) + urlSuffix)
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
