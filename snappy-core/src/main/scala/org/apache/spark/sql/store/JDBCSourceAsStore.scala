package org.apache.spark.sql.store

import java.nio.ByteBuffer
import java.sql.{Connection, ResultSet, Statement}
import java.util.{Properties, UUID}
import java.util.concurrent.locks.ReentrantLock

import org.apache.spark.rdd.{RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.UUIDRegionKey
import org.apache.spark.sql.columnar.{ConnectionProperties, CachedBatch, ExternalStoreUtils}
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.{Partitioner, HashPartitioner, TaskContext, Partition, SparkContext, SparkEnv}
import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random


/*
Generic class to query column table from Snappy.
 */
class JDBCSourceAsStore(override val connProperties:ConnectionProperties,
    numPartitions:Int) extends ExternalStore {

  @transient
  protected lazy val serializer = SparkEnv.get.serializer

  @transient
  protected lazy val rand = new Random

  protected val dialect = JdbcDialects.get(connProperties.url)

  lazy val connectionType = ExternalStoreUtils.getConnectionType(connProperties.url)

  def getCachedBatchRDD(tableName: String,
      requiredColumns: Array[String],
      sparkContext: SparkContext): RDD[CachedBatch] = {
    new ExternalStorePartitionedRDD(sparkContext, tableName, requiredColumns, numPartitions, this)
  }

  override def storeCachedBatch(tableName: String, batch: CachedBatch,
      bucketId: Int = -1, batchId: Option[UUID] = None): UUIDRegionKey = {
    val uuid = getUUIDRegionKey(tableName, bucketId, batchId)
    storeCurrentBatch(tableName, batch, uuid)
    uuid
  }

  override def getUUIDRegionKey(tableName: String, bucketId: Int = -1,
      batchId: Option[UUID] = None): UUIDRegionKey = {
    genUUIDRegionKey(rand.nextInt(numPartitions))
  }

  def storeCurrentBatch(tableName: String, batch: CachedBatch, uuid: UUIDRegionKey): Unit = {
    tryExecute(tableName, {
      case connection =>
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
    })

  }

  override def getConnection(id: String): Connection = {
    ConnectionPool.getPoolConnection(id, None, dialect, connProperties.poolProps,
      connProperties.connProps, connProperties.hikariCP)
  }

  protected def genUUIDRegionKey(bucketId: Int = -1) = new UUIDRegionKey(bucketId)

  protected def genUUIDRegionKey(bucketID: Int, batchID: UUID) = new UUIDRegionKey(bucketID, batchID)

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
        s"insert into $tableName values(?,?,?,?${",?" * numOfColumns})")
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
      }
    }
  }

  private def getCachedBatchFromRow(requiredColumns: Array[String],
      rs: ResultSet): CachedBatch = {
    // it will be having the information of the columns to fetch
    val numCols = requiredColumns.length
    val colBuffers = new Array[Array[Byte]](numCols)
    var i = 0
    while (i < numCols) {
      colBuffers(i) = rs.getBytes(i + 1)
      i += 1
    }
    val stats = serializer.newInstance().deserialize[InternalRow](ByteBuffer.
        wrap(rs.getBytes("stats")))

    CachedBatch(rs.getInt("numRows"), colBuffers, stats)
  }

}

class ExternalStorePartitionedRDD[T: ClassTag](@transient _sc: SparkContext,
                                               tableName: String, requiredColumns: Array[String],
                                               numPartitions: Int,
                                               store: JDBCSourceAsStore)
  extends RDD[CachedBatch](_sc, Nil) {

  override def compute(split: Partition,
                       context: TaskContext): Iterator[CachedBatch] = {
    store.tryExecute(tableName, {
      case conn =>

        val resolvedName = {
          if (tableName.indexOf(".") <= 0) {
            conn.getSchema + "." + tableName
          } else tableName
        }.toUpperCase

        val par = split.index
        val stmt = conn.createStatement()
        val query = "select " + requiredColumns.mkString(", ") +
          s", numRows, stats from $tableName where bucketid = $par"
        val rs = stmt.executeQuery(query)
        new CachedBatchIteratorOnRS(conn, requiredColumns, stmt, rs)
    }, closeOnSuccess = false)
  }

  override protected def getPartitions: Array[Partition] = {
    for (p <- 0 until numPartitions) {
      partitions(p) = new Partition {
        override def index: Int = p
      }
    }
    partitions
  }

}