package org.apache.spark.sql.store.impl

import java.io.{DataInputStream, ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream}
import java.sql.{ResultSet, Statement, Connection, Blob, PreparedStatement}
import java.util.concurrent.locks.ReentrantLock

import com.gemstone.gemfire.internal.cache.{AbstractRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.impl.jdbc.{EmbedBlob, EmbedConnection}
import org.apache.spark.sql.Row
import org.apache.spark.sql.collection.UUIDRegionKey
import org.apache.spark.sql.columnar.ConnectionType.ConnectionType

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import org.apache.spark.SparkEnv

import org.apache.spark.sql.columnar.{ConnectionType, ExternalStoreUtils, CachedBatch}
import org.apache.spark.sql.store.ExternalStore

import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.Random

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
  private lazy val connectionType = ExternalStoreUtils.getConnectionType(url)

  override def initSource() = {

  }

  override def storeCachedBatch(batch: CachedBatch, tableName: String): UUIDRegionKey = {
    val connection: java.sql.Connection = getConnection(tableName)
    try {
      val uuid = connectionType match {
        case ConnectionType.Embedded => {
          val lookupName = {
            if (tableName.indexOf(".") <= 0) {
              connection.getSchema + "." + tableName
            } else tableName
          }.toUpperCase

          val region = Misc.getRegionForTable(lookupName, true)
          region.asInstanceOf[AbstractRegion] match {
            case pr: PartitionedRegion =>
              val primaryBuckets = pr.getDataStore.getAllLocalPrimaryBucketIds.toArray(new Array[Integer](0))
              genUUIDRegionKey(rand.nextInt(primaryBuckets.size))
            case _ =>
              genUUIDRegionKey()
          }
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
      val result = stmt.executeUpdate()
      stmt.close()
      uuid
    } finally {
      connection.close()
    }
  }


  override def cleanup(): Unit = {

  }

  override def truncate(tableName: String) = tryExecute(tableName, {
    case conn => {
      val st = conn.createStatement()
      st.executeQuery(s"truncate table $tableName")
      st.close()
    }
  })

  override def getCachedBatchIterator(tableName: String,
                                      itr: Iterator[UUIDRegionKey], getAll: Boolean = false): Iterator[CachedBatch] = {

    itr.sliding(10, 10).flatMap(kIter => tryExecute(tableName, {
      case conn =>
          val (uuidIter, bucketIter) = kIter.map(k => k.getUUID -> k.getBucketId).unzip

          val uuidParams = uuidIter.foldRight(new StringBuilder)({ case (_, o) => o.append("?,") })
          if (uuidParams.nonEmpty) {
            uuidParams.setCharAt(uuidParams.length - 1, ' ')
          }
          else {
            return Iterator.empty
          }

          val ps = conn match {
            case eC: EmbedConnection =>
              val bucketParams = bucketIter.foldRight(new StringBuilder)({ case (_, o) => o.append("?,") })
              if (bucketParams.nonEmpty) bucketParams.setCharAt(bucketParams.length - 1, ' ')

              val ps = conn.prepareStatement(s"select cachedBatch from $tableName where uuid IN ($uuidParams) " +
                s"and bucketId IN ($bucketParams)")

              uuidIter.zipWithIndex.foreach({
                case (_id, idx) => ps.setString(idx + 1, _id.toString)
              })

              val offset = uuidIter.length

              bucketIter.zipWithIndex.foreach({
                case (_bucket, idx) => ps.setInt(idx + offset + 1, _bucket)
              })
              ps

            case _ =>
              val ps = conn.prepareStatement(s"select cachedBatch from $tableName where uuid IN ($uuidParams)")

              uuidIter.zipWithIndex.foreach({
                case (_id, idx) => ps.setString(idx + 1, _id.toString)
              })
              ps
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
    // TODO: KN look at pool later
    ExternalStoreUtils.getPoolConnection(id, None, poolProps, connProps, _hikariCP)
//    ExternalStoreUtils.getConnection(url, connProps)
  }

  private def genUUIDRegionKey(bucketId: Integer = -1) = new UUIDRegionKey(bucketId)

  private var insertStrings: mutable.HashMap[String, String] =
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
    val deserializationStream = serializer.newInstance.deserializeStream(new ByteArrayInputStream(bytes))
    val stats = deserializationStream.readValue[Row]()
    blob.free()
    CachedBatch(colBuffers.toArray, stats)
  }

}
