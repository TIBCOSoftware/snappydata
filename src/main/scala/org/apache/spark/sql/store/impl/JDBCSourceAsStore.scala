package org.apache.spark.sql.store.impl

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.sql.{Blob, Connection, PreparedStatement, ResultSet}
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random

import com.gemstone.gemfire.cache.{EntryOperation, PartitionResolver}
import com.gemstone.gemfire.internal.cache.{AbstractRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql.catalyst.expressions.InternalRow
import org.apache.spark.sql.collection.{ExecutorLocalPartition, UUIDRegionKey}
import org.apache.spark.sql.columnar.ConnectionType.ConnectionType
import org.apache.spark.sql.columnar.{CachedBatch, ConnectionType, ExternalStoreUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.{JdbcExtendedDialect, JdbcExtendedUtils}
import org.apache.spark.sql.store.ExternalStore
import org.apache.spark.{Partition, SparkContext, SparkEnv, TaskContext}

/**
 * ExternalStore implementation for GemFireXD.
 *
 * Created by Neeraj on 16/7/15.
 */
final class JDBCSourceAsStore(jdbcSource: Map[String, String])  extends ExternalStore {

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

  def lookupName(tableName :String, schema : String): String ={
    val lookupName = {
      if (tableName.indexOf(".") <= 0) {
        schema + "." + tableName
      } else tableName
    }.toUpperCase
    lookupName
  }

  def getCachedBatchRDD(tableName: String,
      uuidList: ArrayBuffer[RDD[UUIDRegionKey]],
      sparkContext: SparkContext): RDD[CachedBatch] = {
    val connection: java.sql.Connection = getConnection(tableName)
    connectionType match {
      case ConnectionType.Embedded =>
        new ExternalStorePartitionedRDD[CachedBatch](sparkContext,
          connection.getSchema, tableName, this)
      case _ =>
        var rddList = new ArrayBuffer[RDD[CachedBatch]]()
        uuidList.foreach(x => {
          val y = x.mapPartitions { uuidItr =>
            getCachedBatchIterator(tableName, uuidItr)
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
          val resolvedName = lookupName(tableName,connection.getSchema)
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

      val blob = prepareCachedBatchAsBlob(batch, connection)
      val rowInsertStr = getRowInsertStr(tableName)
      val stmt = connection.prepareStatement(rowInsertStr)
      stmt.setString(1, uuid.getUUID.toString)
      stmt.setInt(2, uuid.getBucketId)
      //stmt.setBlob(3, blob)
      stmt.setBytes(3, blob)
      stmt.executeUpdate()
      stmt.close()
      uuid
    } finally {
      connection.close()
    }
  }

  override def cleanup(): Unit = {

  }

  override def truncate(tableName: String) = tryExecute(tableName, {
    case conn =>
    dialect match {
      case d: JdbcExtendedDialect => d.truncateTable(tableName)
      case _ =>
        JdbcExtendedUtils.executeUpdate(s"truncate table $tableName", conn)
    }
  })

  override def getCachedBatchIterator(tableName: String,
      itr: Iterator[UUIDRegionKey], getAll: Boolean = false): Iterator[CachedBatch] = {

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
          s"select cachedBatch from $tableName where uuid IN ($uuidParams)")

        uuidIter.zipWithIndex.foreach {
          case (_id, idx) => ps.setString(idx + 1, _id.toString)
        }
        val rs = ps.executeQuery()

        new CachedBatchIteratorFromRS(conn, connectionType, ps, rs)
    }, closeOnSuccess = false))
  }


  private def prepareCachedBatchAsBlob(batch: CachedBatch, conn: Connection) = {
    val outputStream = new ByteArrayOutputStream()
    val dos = new DataOutputStream(outputStream)

    val numCols = batch.buffers.length
    dos.writeInt(numCols)

    batch.buffers.foreach(x => {
      dos.writeInt(x.length)
      dos.write(x)
    })
    val ser = serializer.newInstance()
    val bf = ser.serialize(batch.stats)
    dos.write(bf.array())
    // println("KN: length of blob put = " + blob.length() + " lenght of serialized bf: " + bf.array().length)
    outputStream.toByteArray
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

  private def getRowInsertStr(tableName: String): String = {
    val istr = insertStrings.getOrElse(tableName, {
      lock(makeInsertStmnt(tableName))
    })
    istr
  }

  private def makeInsertStmnt(tableName: String) = {
    if (!insertStrings.contains(tableName)) {
      val s = insertStrings.getOrElse(tableName, s"insert into $tableName values(?, ?, ?)")
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

private final class CachedBatchIteratorFromRS(conn: Connection, connType: ConnectionType,
                                             ps: PreparedStatement, rs: ResultSet) extends Iterator[CachedBatch] {

  private val serializer = SparkEnv.get.serializer
  var _hasNext = moveNext()

  override def hasNext: Boolean = _hasNext

  override def next() = {
    val result = getCachedBatchFromBlob(rs.getBlob(1), connType)
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

  private def getCachedBatchFromBlob(blob: Blob, connType: ConnectionType): CachedBatch = {
    val totBytes = blob.length().toInt
//    println("KN: length of blob get = " + blob.length())
    val bis = connType match {
      case ConnectionType.Embedded =>
        blob.getBinaryStream
      case _ =>
        new ByteArrayInputStream(blob.getBytes(1, totBytes))
    }
    val dis = new DataInputStream(bis)
    var offset = 0
    val numCols = dis.readInt()
    offset = offset + 4
    val colBuffers = new ArrayBuffer[Array[Byte]]()
    for (i <- 0 until numCols) {
      val lenOfByteArr = dis.readInt()
      offset = offset + 4
      val colBuffer = new Array[Byte](lenOfByteArr)
      dis.read(colBuffer)
      offset = offset + lenOfByteArr
      colBuffers += colBuffer
    }
    val remainingLength = totBytes - offset
    val bytes = new Array[Byte](remainingLength)
    dis.read(bytes)
    val deserializationStream = serializer.newInstance().deserializeStream(
      new ByteArrayInputStream(bytes))
    val stats = deserializationStream.readValue[InternalRow]()
    blob.free()
    CachedBatch(colBuffers.toArray, stats)
  }

}

final class UUIDKeyResolver extends PartitionResolver[UUIDRegionKey, CachedBatch] {

  override def getRoutingObject(entryOperation: EntryOperation[UUIDRegionKey, CachedBatch]): AnyRef = {
    Int.box(entryOperation.getKey.getBucketId)
  }

  override def getName: String = "UUIDKeyResolver"

  override def close(): Unit = {}
}

class ExternalStorePartitionedRDD[T: ClassTag](@transient _sc: SparkContext, schema: String, tableName: String, store: JDBCSourceAsStore)
    extends RDD[CachedBatch](_sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[CachedBatch] = {
    store.tryExecute(tableName, {
      case conn =>
        conn.setTransactionIsolation(Connection.TRANSACTION_NONE)
        val resolvedName = store.lookupName(tableName, schema)
        //val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]
        val par = split.index
        val ps1 = conn.prepareStatement(s"call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION('$resolvedName', $par)")
        ps1.execute()
        val ps = conn.prepareStatement(s"select cachedBatch from $tableName")

        val rs = ps.executeQuery()

        new CachedBatchIteratorFromRS(conn, store.connectionType, ps, rs)
    }, closeOnSuccess = false)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[ExecutorLocalPartition].hostExecutorId)
  }

  override protected def getPartitions: Array[Partition] = {
    val resolvedName = store.lookupName(tableName, schema)
    val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]
    val numPartitions = region.getTotalNumberOfBuckets
    val partitions = new Array[Partition](numPartitions)

    val numberedPeers = org.apache.spark.sql.collection.Utils.getAllExecutorsMemoryStatus(sparkContext).
        keySet.zipWithIndex
    val hostSet = numberedPeers.map(m => {
      Tuple2(m._1.host , m._1)
    }).toMap

    for (p <- 0 until numPartitions) {
      //TODO there should be a cleaner way to translate GemFire membership IDs to BlockManagerIds
      //TODO apart from primary members secondary nodes should also be included in preferred node list
      val distMember = region.getBucketPrimary(p)
      val prefNode = hostSet.get(distMember.getIpAddress.getHostAddress)
      partitions(p) = new ExecutorLocalPartition(p, prefNode.get)
    }
    partitions
  }
}
