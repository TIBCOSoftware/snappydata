package org.apache.spark.sql.store.impl

import java.sql.{Blob, PreparedStatement, DriverManager, Connection}
import java.util.UUID
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark.SparkContext
import org.apache.spark.sql.{AnalysisException, SnappyContext}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.store.ExternalStore

import scala.collection.mutable.HashMap

/**
 * Created by neeraj on 16/7/15.
 */
class GemXDSource_LC extends ExternalStore {

  lazy val connURL: String = getConnURL

  private val cachedConn: Connection = createCachedConnection

  private val connLock = new ReentrantReadWriteLock()

  private val preparedStmntCache = new HashMap[String,PreparedStatement]()

  /** Acquires a read lock on the conn for the duration of `f`. */
  private[sql] def readLock[A](f: => A): A = {
    val lock = connLock.readLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Acquires a write lock on the cache for the duration of `f`. */
  private[sql] def writeLock[A](f: => A): A = {
    val lock = connLock.writeLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  override def initSource() = {
    if (connURL == null && connURL.isEmpty) {
      throw new IllegalStateException(
        s"Invalid connURL for GemXD store $connURL")
    }
  }

  private def createCachedConnection: Connection = DriverManager.getConnection(connURL)

  implicit def uuidToString(uuid: UUID): String = {
    uuid.toString
  }

  implicit def cachedBatchToBlob(batch: CachedBatch): Blob = {
    // TODO: write proper method
    Nil.asInstanceOf[Blob]
  }

  override def storeCachedBatch(batch: CachedBatch, tname: String): Any = writeLock {
    var ps = this.preparedStmntCache.getOrElse(tname, preparePrepStmnt(tname))
    val uuid = UUID.randomUUID()
    ps.setString(1, uuid)
    ps.setBlob(2, batch)
    uuid.toString
  }

  override def cleanup(): Unit = {
    this.preparedStmntCache.values.foreach(ps => ps.close())
    cachedConn.close()
  }

  private def preparePrepStmnt(tname: String): PreparedStatement = {
    val pInsert = cachedConn.prepareStatement("insert into " + tname + " values (?, ?)")
    this.preparedStmntCache.put(tname, pInsert)
    pInsert
  }

  private def getConnURL(): String = {
    val sc = SparkContext.getOrCreate()
    sc match {
      case snc: SnappyContext => {
        val config = snc.getExternalStoreConfig
        val host = config.getOrElse("host", "localhost")
        val port = config.getOrElse("port", "1527")
        "jdbc:gemfirexd://" + host + ":" + port + "/"
      }
      case sc =>
        throw new IllegalStateException("Extended snappy operations " +
          s"require SnappyContext and not ${sc.getClass.getSimpleName}")
    }
  }
}
