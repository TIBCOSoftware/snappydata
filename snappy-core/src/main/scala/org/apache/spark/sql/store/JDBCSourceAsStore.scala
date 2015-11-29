package org.apache.spark.sql.store

import java.nio.ByteBuffer
import java.sql.{Connection, ResultSet, Statement}
import java.util.{Properties, UUID}
import java.util.concurrent.locks.ReentrantLock

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.UUIDRegionKey
import org.apache.spark.sql.columnar.{CachedBatch, ExternalStoreUtils}
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.{SparkContext, SparkEnv}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.Random

/*
Generic class to query column table from Snappy.
 */
class JDBCSourceAsStore(override val url: String,
    override val driver: String,
    override val poolProps: Map[String, String],
    override val connProps: Properties,
    val hikariCP: Boolean) extends ExternalStore {

  @transient
  protected lazy val serializer = SparkEnv.get.serializer

  @transient
  protected lazy val rand = new Random

  protected val dialect = JdbcDialects.get(url)

  lazy val connectionType = ExternalStoreUtils.getConnectionType(url)

  def getCachedBatchRDD(tableName: String,
      requiredColumns: Array[String],
      uuidList: ArrayBuffer[RDD[UUIDRegionKey]],
      sparkContext: SparkContext): RDD[CachedBatch] = {
    var rddList = new ArrayBuffer[RDD[CachedBatch]]()
    uuidList.foreach(x => {
      val y = x.mapPartitions { uuidItr =>
        getCachedBatchIterator(tableName, requiredColumns, uuidItr)
      }
      rddList += y
    })
    new UnionRDD[CachedBatch](sparkContext, rddList)
  }

  override def storeCachedBatch(batch: CachedBatch,
      tableName: String): UUIDRegionKey = {
    tryExecute(tableName, {
      case connection =>
        val uuid = genUUIDRegionKey()
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
    })
  }

  override def storeCachedBatch(batch: CachedBatch, batchID: UUID, bucketId: Int, tableName: String): UUIDRegionKey = {
    tryExecute(tableName, {
      case connection =>
        val uuid = genUUIDRegionKey(bucketId, batchID)
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
    })
  }

  override def getCachedBatchIterator(tableName: String, requiredColumns: Array[String],
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

        new CachedBatchIteratorOnRS(conn, requiredColumns, ps, rs)
    }, closeOnSuccess = false))
  }

  implicit def uuidToString(uuid: UUIDRegionKey): String = {
    uuid.toString
  }

  override def getConnection(id: String): Connection = {
    ConnectionPool.getPoolConnection(id, None, dialect, poolProps,
      connProps, hikariCP)
  }

  protected def genUUIDRegionKey(bucketId: Int = -1) = new UUIDRegionKey(bucketId)

  protected def genUUIDRegionKey(bucketID: Int , batchID: UUID) = new UUIDRegionKey(bucketID, batchID)

  protected val insertStrings: mutable.HashMap[String, String] =
    new mutable.HashMap[String, String]()

  protected def getRowInsertStr(tableName: String, numOfColumns: Int): String = {
    val istr = insertStrings.getOrElse(tableName, {
      lock(makeInsertStmnt(tableName, numOfColumns))
    })
    istr
  }

  protected def makeInsertStmnt(tableName: String, numOfColumns: Int) = {
    if (!insertStrings.contains(tableName)) {
      val s = insertStrings.getOrElse(tableName,
        s"insert into $tableName values(?, ? , ? " + ",?" * numOfColumns + ")")
      insertStrings.put(tableName, s)
    }
    insertStrings.get(tableName).get
  }

  protected val insertStmntLock = new ReentrantLock()

  /** Acquires a read lock on the cache for the duration of `f`. */
  protected[sql] def lock[A](f: => A): A = {
    insertStmntLock.lock()
    try f finally {
      insertStmntLock.unlock()
    }
  }
}

final class CachedBatchIteratorOnRS(conn: Connection,
    requiredColumns: Array[String],
    ps: Statement, rs: ResultSet) extends Iterator[CachedBatch] {

  private val serializer = SparkEnv.get.serializer
  var _hasNext = moveNext()

  override def hasNext: Boolean = _hasNext

  override def next() = {
    val result = getCachedBatchFromRow(requiredColumns, rs)
    _hasNext = moveNext()
    result
  }

  private def moveNext(): Boolean = {
    var success = false
    try {
      success = rs.next()
      success
    } finally {
      if (!success) {
        rs.close()
        ps.close()
        conn.close()
        false
      }
    }
  }

  private def getCachedBatchFromRow(requiredColumns: Array[String],
      rs: ResultSet): CachedBatch = {
    // it will be having the information of the columns to fetch
    val numCols = requiredColumns.length
    val colBuffers = new ArrayBuffer[Array[Byte]]()
    for (i <- 0 until numCols) {
      colBuffers += rs.getBytes(requiredColumns(i)).array
    }
    val stats = serializer.newInstance().deserialize[InternalRow](ByteBuffer.
        wrap(rs.getBytes("stats")))

    CachedBatch(colBuffers.toArray, stats)
  }
}
