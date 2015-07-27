package org.apache.spark.sql.store.impl

import java.io.{DataInputStream, ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream}
import java.sql.{Statement, Connection, Blob}

import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection
import org.apache.spark.sql.Row
import org.apache.spark.sql.collection.UUIDRegionKey

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import org.apache.spark.SparkEnv

import org.apache.spark.sql.columnar.{ExternalStoreUtils, CachedBatch}
import org.apache.spark.sql.store.ExternalStore

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random

/**
 * ExternalStore implementation for GemFireXD.
 *
 * Created by Neeraj on 16/7/15.
 */
final class JDBCSourceAsStore(jdbcSource: Map[String, String]) extends ExternalStore {

  private val serializer = SparkEnv.get.serializer

  private val rand = new Random

  private lazy val (url, driver, poolProps, connProps, hikariCP) = ExternalStoreUtils.validateAndGetAllProps(jdbcSource)

  override def initSource() = {

  }

  override def storeCachedBatch(batch: CachedBatch, tableName: String): UUIDRegionKey = {
    val connection = getConnection(tableName)
    val blob = prepareCachedBatchAsBlob(batch, connection)
    try {
      val uuid = connection match {
        case embedConn: EmbedConnection => {
          val region = Misc.getRegionForTable(tableName, true)
          if (region.isInstanceOf[PartitionedRegion]) {
            val pr: PartitionedRegion = region.asInstanceOf[PartitionedRegion]
            val primaryBuckets = pr.getDataStore.getAllLocalPrimaryBucketIds.toArray(new Array[Integer](10))
            genUUIDRegionKey(rand.nextInt(primaryBuckets.size))
          }
          else {
            genUUIDRegionKey()
          }
        }
        case _ => genUUIDRegionKey()
      }

      val rowInsertStr = getRowInsertStr(tableName)
      val stmt = connection.prepareStatement(rowInsertStr)
      stmt.setString(1, uuid.getUUID.toString)
      stmt.setInt(2, uuid.getBucketId)
      stmt.setBlob(3, blob)
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
    case (conn, st) => {
      st.executeQuery(s"truncate table $tableName")
      st.close()
      conn.close()
    }
  })

  override def getCachedBatchIterator(tableName: String,
                                      itr: Iterator[UUIDRegionKey], getAll: Boolean = false): Iterator[CachedBatch] = {
    val (uuid, bucketId) = itr.foldLeft((new StringBuilder("'"), new StringBuilder)) { (output, key) =>
      output match {
        case (uuid, bucketId) =>
          uuid.append(key.getUUID).append("','")
          bucketId.append(key.getBucketId).append(",")
      }
      output
    }

    if (bucketId.isEmpty) {
      return Iterator.empty
    }

    uuid.append("'")
    bucketId.append(-1)

    tryExecute(tableName, {
      case (conn, st) =>
        val rs = st.executeQuery(s"select cachedBatch from $tableName where uuid IN (${uuid.toString}) " +
          s"and bucketId IN (${bucketId.toString})")
        new Iterator[CachedBatch] {
          override def hasNext: Boolean = rs.next() match {
            case false =>
              rs.close()
              st.close()
              false
            case _ => true
          }

          override def next() = getCachedBatchFromBlob(rs.getBlob(1))
        }

    })
  }


  private def prepareCachedBatchAsBlob(batch: CachedBatch, conn: Connection): Blob = {
    val outputStream = new ByteArrayOutputStream()
    val dos = new DataOutputStream(outputStream)
    val blob = conn.createBlob()
    val numCols = batch.buffers.length
    dos.writeInt(numCols)

    batch.buffers.foreach(x => {
      dos.writeInt(x.length)
      dos.write(x)
    })
    val ser = serializer.newInstance()
    val bf = ser.serialize(batch.stats)
    dos.write(bf.array())
    val barr = outputStream.toByteArray
    blob.setBytes(0, barr)
    blob
  }

  private def getCachedBatchFromBlob(blob: Blob): CachedBatch = {
    val totBytes = blob.length()
    val dis = new DataInputStream(blob.getBinaryStream)
    var offset = 0
    val numCols = dis.readInt()
    offset = offset + 4
    val colBuffers = new ArrayBuffer[Array[Byte]]()
    for (i <- 0 until numCols) {
      val lenOfByteArr = dis.readInt()
      offset = offset + 4
      val colBuffer = Array.fill[Byte](lenOfByteArr)(0)
      dis.read(colBuffer)
      offset = offset + lenOfByteArr
      colBuffers += colBuffer
    }
    val remainingLength = totBytes - offset
    val bytes = Array.fill[Byte](remainingLength.asInstanceOf[Int])(0)
    dis.read(bytes)
    val deserializationStream = serializer.newInstance.deserializeStream(new ByteArrayInputStream(bytes))
    val stats = deserializationStream.readValue[Row]()
    CachedBatch(colBuffers.toArray, stats)
  }

  implicit def uuidToString(uuid: UUIDRegionKey): String = {
    uuid.toString
  }

  private val insertStrings: mutable.HashMap[String, String] =
    new mutable.HashMap[String, String]()

  private def getRowInsertStr(tableName: String): String = {
    insertStrings.getOrElse(tableName, {
      synchronized(insertStrings) {
        val s = insertStrings.getOrElse(tableName, s"insert into $tableName values(?, ?, ?")
        insertStrings.put(tableName, s)
        s
      }
    })
  }

  private def getConnection(tableName: String): Connection = {
    ExternalStoreUtils.getPoolConnection(tableName, driver, poolProps, connProps, hikariCP)
  }

  private def genUUIDRegionKey(bucketId: Integer = 0) = new UUIDRegionKey(bucketId)

  private def tryExecute[T: ClassTag](tableName: String, f: PartialFunction[(Connection, Statement), T]): T = {
    val conn = getConnection(tableName)
    val st = conn.createStatement()
    try {
      f(conn, st)
    } catch {
      case t: Throwable => {
        st.close()
        conn.close()
        throw t;
      }
    }
  }

}
