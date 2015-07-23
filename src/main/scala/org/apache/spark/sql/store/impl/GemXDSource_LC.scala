package org.apache.spark.sql.store.impl

import java.io.{DataInputStream, ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream}
import java.sql.{Blob, Connection, DriverManager, PreparedStatement}
import java.util.{Properties, UUID}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.ConnectionPool

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import org.apache.spark.{SparkEnv, SparkConf, SparkContext}

import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.store.ExternalStore

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * ExternalStore implementation for GemFireXD.
 *
 * Created by Neeraj on 16/7/15.
 */
class GemXDSource_LC(connURL: String, connProperties: Properties, poolProperties: Map[String, String], hikariCP: Boolean) extends ExternalStore {

private val serializer = SparkEnv.get.serializer

  override def initSource() = {

  }

  implicit def uuidToString(uuid: UUID): String = {
    uuid.toString
  }

  private var insertStrings: mutable.HashMap[String, String] =
    new mutable.HashMap[String, String]()

  private def getRowInsertStr(tableName: String): String = {
    val istr = insertStrings.getOrElse(tableName, {
      synchronized(insertStrings) {
        val s = insertStrings.getOrElse(tableName, s"insert into $tableName values(?, ?")
        insertStrings.put(tableName, s)
        s
      }
    })
    istr
  }

  override def storeCachedBatch(batch: CachedBatch, tableName: String): UUID = {
    val connection = ConnectionPool.getPoolConnection(tableName,
      poolProperties, connProperties, hikariCP)
    val ser = serializer.newInstance()
    var blob = prepareCachedBatchAsBlob(batch, connection)
    try {
      val uuid = UUID.randomUUID()
      val rowInsertStr = getRowInsertStr(tableName)
      val stmt = connection.prepareStatement(rowInsertStr)
      stmt.setString(1, uuid)
      stmt.setBlob(2, blob)
      val result = stmt.executeUpdate()
      stmt.close()
      uuid
    } finally {
      connection.close()
    }
  }

  private def prepareCachedBatchAsBlob(batch: CachedBatch, conn: Connection): Blob = {
    var baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    var blob = conn.createBlob()
    val numCols = batch.buffers.length
    dos.writeInt(numCols)

    batch.buffers.foreach(x => {
      var len = x.length
      dos.writeInt(len)
      dos.write(x)
    })
    val ser = serializer.newInstance()
    val bf = ser.serialize(batch.stats)
    dos.write(bf.array())
    val barr = baos.toByteArray
    blob.setBytes(0, barr)
    blob
  }

  private def getCachedBatchFromBlob(blob: Blob): CachedBatch = {
    val totBytes = blob.length()
    var inpstream = blob.getBinaryStream
    var dis = new DataInputStream(inpstream)
    var offset = 0
    val numCols = dis.readInt()
    offset = offset + 4
    val colBuffers = new ArrayBuffer[Array[Byte]]()
    for(i <- 0 until numCols) {
      val lenOfByteArr = dis.readInt()
      offset = offset + 4
      val colbuf = Array.fill[Byte](lenOfByteArr)(0)
      val length = dis.read(colbuf)
      offset = offset + lenOfByteArr
      colBuffers += colbuf
    }
    val remainingLength = totBytes - offset
    val bytes = Array.fill[Byte](remainingLength.asInstanceOf[Int])(0)
    val length = dis.read(bytes)
    val deserializationStream = serializer.newInstance.deserializeStream(new ByteArrayInputStream(bytes))
    val stats = deserializationStream.readValue[Row]()
    CachedBatch(colBuffers.toArray, stats)
  }

  override def cleanup(): Unit = {

  }

  override  def truncate(tableName: String) = {

  }

  override def getCachedBatchIterator(tableName: String,
   itr: Iterator[UUID], getAll: Boolean = false): Iterator[CachedBatch] = {
    val connection = ConnectionPool.getPoolConnection(tableName,
      poolProperties, connProperties, hikariCP)
    val sb = new StringBuilder()
    if (getAll) {
      val alluuids = itr.foreach(u => {
        sb.append(u)
        sb.append(',')
      })
      if (!sb.isEmpty) {
        sb.deleteCharAt(sb.length-1)
      }
      else {
        new Iterator[CachedBatch] {

          override def hasNext: Boolean = false

          override def next() = {
            null
          }
        }
      }
      val rs = connection.createStatement().
        executeQuery(s"select cachedbatch from $tableName where uuid IN (${sb.toString()})")
        new Iterator[CachedBatch] {

          override def hasNext: Boolean = rs.next()

          override def next() = {
            val blob = rs.getBlob(1)
            getCachedBatchFromBlob(blob)
          }
        }
      }
     else {
      val ps = connection.prepareStatement(s"select cachedbatch from $tableName where uuid = ?")

      new Iterator[CachedBatch] {

        override def hasNext: Boolean = itr.hasNext

        override def next() = {
          val u = itr.next()
          ps.setString(1, u)
          val rs = ps.executeQuery()
          val blob = rs.getBlob(1)
          getCachedBatchFromBlob(blob)
        }
      }
    }
  }
}
