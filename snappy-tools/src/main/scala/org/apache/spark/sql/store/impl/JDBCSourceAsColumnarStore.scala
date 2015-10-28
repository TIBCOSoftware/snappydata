package org.apache.spark.sql.store.impl

import java.nio.ByteBuffer
import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.concurrent.locks.ReentrantLock

import com.gemstone.gemfire.internal.cache.{AbstractRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.{ExecutorLocalPartition, UUIDRegionKey}
import org.apache.spark.sql.columnar.ConnectionType.ConnectionType
import org.apache.spark.sql.columnar.{CachedBatch, ConnectionType, ExternalStoreUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.{JdbcExtendedDialect, JdbcExtendedUtils}
import org.apache.spark.sql.store.ExternalStore
import org.apache.spark.{Partition, SparkContext, SparkEnv, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random

/**
 * Columnar Store implementation for GemFireXD.
 *
 */
final class JDBCSourceAsColumnarStore(jdbcSource: Map[String, String]) extends ExternalStore {

  @transient
  private lazy val serializer = SparkEnv.get.serializer

  @transient
  private lazy val rand = new Random

  private val (_url, _driver, _poolProps, _connProps, _hikariCP) = ExternalStoreUtils.validateAndGetAllProps(jdbcSource)

  @transient
  private val dialect = JdbcDialects.get(url)

  @transient
  lazy val connectionType = ExternalStoreUtils.getConnectionType(url)

  override def initSource() = {

  }

  def lookupName(tableName: String, schema: String): String = {
    val lookupName = {
      if (tableName.indexOf(".") <= 0) {
        schema + "." + tableName
      } else tableName
    }.toUpperCase
    lookupName
  }

  def getCachedBatchRDD(tableName: String, requiredColumns: Array[String],
      uuidList: ArrayBuffer[RDD[UUIDRegionKey]],
      sparkContext: SparkContext): RDD[CachedBatch] = {
    val connection: java.sql.Connection = getConnection(tableName)
    connectionType match {
      case ConnectionType.Embedded =>
        new ColumnarStorePartitionedRDD[CachedBatch](sparkContext,
          connection.getSchema, tableName, requiredColumns, this)
      case _ =>
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

  override def storeCachedBatch(batch: CachedBatch,
      tableName: String): UUIDRegionKey = {
    val connection: java.sql.Connection = getConnection(tableName)
    try {
      val uuid = connectionType match {

        case ConnectionType.Embedded =>
          val resolvedName = lookupName(tableName, connection.getSchema)
          val region = Misc.getRegionForTable(resolvedName, true)
          region.asInstanceOf[AbstractRegion] match {
            case pr: PartitionedRegion =>
              val primaryBuckets = pr.getDataStore.getAllLocalPrimaryBucketIds
                  .toArray(new Array[Integer](0))
              genUUIDRegionKey(rand.nextInt(primaryBuckets.size))
            case _ =>
              genUUIDRegionKey()
          }

        case _ => genUUIDRegionKey()
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

  override def cleanup(): Unit = {

  }

  def getCachedBatchIterator(tableName: String,
      requiredColumns: Array[String],
      itr: Iterator[UUIDRegionKey], getAll: Boolean = false): Iterator[CachedBatch] = {

    //TODO: Suranjan instead of getting 10 batches at a time get one
    //TODO: Suranjan Need to test both the performance
    itr.sliding(10, 10).flatMap(kIter => tryExecute(tableName, {
      case conn =>
        //val (uuidIter, bucketIter) = kIter.map(k => k.getUUID -> k.getBucketId).unzip
        val uuidIter = kIter.map(_.getUUID)

        val uuidParams = uuidIter.foldRight(new StringBuilder) {
          case (_, o) => o.append("?,")
        }
        if (uuidParams.nonEmpty) {
          uuidParams.setCharAt(uuidParams.length - 1, ' ')
        }
        else {
          return Iterator.empty
        }
        val ps = conn.prepareStatement(
          s"select stats," + requiredColumns.mkString(" ", ",", " ") +
              s" from $tableName where uuid IN ($uuidParams)")

        uuidIter.zipWithIndex.foreach {
          case (_id, idx) => ps.setString(idx + 1, _id.toString)
        }
        val rs = ps.executeQuery()

        new CachedBatchIteratorOnRS(conn, connectionType, requiredColumns, ps, rs)
    }, closeOnSuccess = false))
  }

  implicit def uuidToString(uuid: UUIDRegionKey): String = {
    uuid.toString
  }

  override def getConnection(id: String): Connection = {
    ExternalStoreUtils.getPoolConnection(id, None, poolProps, connProps, _hikariCP)
  }

  private def genUUIDRegionKey(bucketId: Int = -1) = new UUIDRegionKey(bucketId)

  private val insertStrings: mutable.HashMap[String, String] =
    new mutable.HashMap[String, String]()

  private def getRowInsertStr(tableName: String, numOfColumns: Int): String = {
    val istr = insertStrings.getOrElse(tableName, {
      lock(makeInsertStmnt(tableName, numOfColumns))
    })
    istr
  }

  private def makeInsertStmnt(tableName: String, numOfColumns: Int) = {
    if (!insertStrings.contains(tableName)) {
      val s = insertStrings.getOrElse(tableName,
        s"insert into $tableName values(?, ? , ? " + ",?" * numOfColumns + ")")
      insertStrings.put(tableName, s)
    }
    insertStrings.get(tableName).get
  }

  private val insertStmntLock = new ReentrantLock()

  /** Acquires a read lock on the cache for the duration of `f`. */
  private[sql] def lock[A](f: => A): A = {
    insertStmntLock.lock()
    try f finally {
      insertStmntLock.unlock()
    }
  }

  override def url = _url

  override def driver = _driver.get

  override def poolProps = _poolProps

  override def connProps = _connProps
}

private final class CachedBatchIteratorOnRS(conn: Connection, connType: ConnectionType,
    requiredColumns: Array[String],
    ps: PreparedStatement, rs: ResultSet) extends Iterator[CachedBatch] {

  private val serializer = SparkEnv.get.serializer
  var _hasNext = moveNext()

  override def hasNext: Boolean = _hasNext

  override def next() = {
    val result = getCachedBatchFromRow(requiredColumns, rs, connType)
    _hasNext = moveNext()
    result
  }

  private def moveNext(): Boolean = {
    if (rs.next()) {
      true
    } else {
      rs.close()
      ps.close()
      conn.close()
      false
    }
  }

  private def getCachedBatchFromRow(requiredColumns: Array[String],
      rs: ResultSet, connType: ConnectionType): CachedBatch = {
    // it will be having the information of the columns to fetch
    val numCols = requiredColumns.length
    val colBuffers = new ArrayBuffer[Array[Byte]]()

    for (i <- 0 until numCols) {
      colBuffers += rs.getBytes(requiredColumns(i)).array
    }

    val stats = serializer.newInstance().deserialize[InternalRow](ByteBuffer.wrap(rs.getBytes("stats")))

    CachedBatch(colBuffers.toArray, stats)
  }

}


class ColumnarStorePartitionedRDD[T: ClassTag](@transient _sc: SparkContext,
    schema: String, tableName: String,
    requiredColumns: Array[String], store: JDBCSourceAsColumnarStore)
    extends RDD[CachedBatch](_sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[CachedBatch] = {
    store.tryExecute(tableName, {
      case conn =>
        //conn.setTransactionIsolation(Connection.TRANSACTION_NONE)
        val resolvedName = store.lookupName(tableName, schema)
        //val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]
        val par = split.index
        //        val ps1 = conn.prepareStatement(s"call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION('$resolvedName', $par)")
        //      ps1.execute()
        val ps = conn.prepareStatement(s"select stats , " +
            requiredColumns.mkString(" ", ",", " ") +
            s" from $tableName")

        val rs = ps.executeQuery()

        new CachedBatchIteratorOnRS(conn, store.connectionType, requiredColumns, ps, rs)
    }, closeOnSuccess = false)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[ExecutorLocalPartition].hostExecutorId)
  }

  override protected def getPartitions: Array[Partition] = {
    val resolvedName = store.lookupName(tableName, schema)
    val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]
    val numPartitions = 1 //region.getTotalNumberOfBuckets
    val partitions = new Array[Partition](numPartitions)

    val numberedPeers = org.apache.spark.sql.collection.Utils.getAllExecutorsMemoryStatus(sparkContext).
        keySet.zipWithIndex
    val hostSet = numberedPeers.map(m => {
      Tuple2(m._1.host, m._1)
    }).toMap

    val localBackend = sparkContext.schedulerBackend match {
      case lb: LocalBackend => true
      case _ => false
    }

    for (p <- 0 until numPartitions) {
      //TODO there should be a cleaner way to translate GemFire membership IDs to BlockManagerIds
      //TODO apart from primary members secondary nodes should also be included in preferred node list
      val distMember = region.getBucketPrimary(p)
      val prefNode = if (localBackend) {
        Option(hostSet.head._2)
      } else {
        hostSet.get(distMember.getIpAddress.getHostAddress)
      }
      partitions(p) = new ExecutorLocalPartition(p, prefNode.get)
    }
    partitions
  }
}
