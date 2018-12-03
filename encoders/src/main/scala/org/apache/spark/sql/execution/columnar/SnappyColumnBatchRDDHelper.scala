/*
 * TODO COMMENT HERE
 */
package org.apache.spark.sql.execution.columnar

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import java.util.{Collections, Properties}

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import io.snappydata.Constant
import io.snappydata.thrift.internal.ClientPreparedStatement

import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.columnar.encoding.ColumnStatsSchema
import org.apache.spark.sql.row.SnappyStoreClientDialect
import org.apache.spark.sql.sources.{ConnectionProperties, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

/**
 *
 * @param tableName
 * @param projection
 * @param schema
 * @param filters
 * @param bucketId
 * @param hostList
 * @param relDestroyVersion
 */
class SnappyColumnBatchRDDHelper(tableName: String, projection: StructType,
    schema: StructType, filters: Option[Array[Filter]], bucketId: Int,
    hostList: ArrayBuffer[(String, String)],
    relDestroyVersion: Int = -1) {

  private var useLocatorURL: Boolean = _
  private var columnBatchIterator: ColumnBatchIteratorOnRS = null
  private var scan_batchNumRows = 0
  private var batchBuffer: ByteBuffer = null

  private val columnOrdinals: Array[Int] = new Array[Int](projection.length)

  private var conn: Connection = _

  /**
   *
   */
  def initialize: Unit = {
    setProjectedColumnOrdinals
    conn = getConnection(getConnProperties, hostList)
    val txId = null
    // fetch all the column blobs pushing down the filters
    val (statement, rs) = prepareScan(conn, txId,
      getTableName, columnOrdinals, serializeFilters, bucketId, relDestroyVersion)
    columnBatchIterator = new ColumnBatchIteratorOnRS(conn, columnOrdinals, statement, rs,
      null, bucketId)
  }

  /**
   *
   * @return
   */
  def next: ColumnarBatch = {

    // Initialize next columnBatch
    val scan_colNextBytes = columnBatchIterator.next()

    // Calculate the number of row in the current batch
    val numStatsColumns = ColumnStatsSchema.numStatsColumns(projection.length)
    val scan_statsRow = org.apache.spark.sql.collection.SharedUtils
        .toUnsafeRow(scan_colNextBytes, numStatsColumns)
    val scan_deltaStatsRow = org.apache.spark.sql.collection.SharedUtils.
        toUnsafeRow(columnBatchIterator.getCurrentDeltaStats, numStatsColumns)
    val scan_batchNumFullRows = scan_statsRow.getInt(0)
    val scan_batchNumDeltaRows = if (scan_deltaStatsRow != null) {
      scan_deltaStatsRow.getInt(0)
    } else 0
    scan_batchNumRows = scan_batchNumFullRows + scan_batchNumDeltaRows

    // Construct ColumnBatch and return
    val columnVectors = new Array[ColumnVector](projection.length)

    // scan_buffer_initialization
    var vectorIndex = 0
    for (columnOrdinal <- columnOrdinals) {
      batchBuffer = columnBatchIterator.getColumnLob(columnOrdinal - 1)
      val field = schema.fields(columnOrdinal - 1)

      val columnVector = new SnappyColumnVector(field.dataType, field,
        batchBuffer, scan_batchNumRows, columnOrdinal)

      columnVectors(vectorIndex) = columnVector
      vectorIndex = vectorIndex + 1
    }
    val columBatch = new ColumnarBatch(columnVectors)
    columBatch.setNumRows(scan_batchNumRows)
    columBatch
  }

  /**
   *
   * @return
   */
  def hasNext : Boolean = {
    columnBatchIterator.hasNext
  }

  /**
   *
   */
  def close : Unit = {
    columnBatchIterator.close()
  }

  /**
   * Get the actual table name created inside the gemxd layer
   *
   * @return
   */
  private def getTableName: String = {
    val dotIndex = tableName.indexOf('.')
    val schema = tableName.substring(0, dotIndex)
    val table = if (dotIndex > 0) tableName.substring(dotIndex + 1) else tableName
    schema + '.' + Constant.SHADOW_SCHEMA_NAME_WITH_SEPARATOR +
        table + Constant.SHADOW_TABLE_SUFFIX
  }

  /**
   * Method takes in projection column schema and calculates ordinals
   * of the projected columns
   *
   * @return
   */
  private def setProjectedColumnOrdinals: Unit = {
    var ordinal = 0
    for (field <- projection.fields){
      columnOrdinals(ordinal) = schema.fieldIndex(field.name) + 1
      ordinal = ordinal + 1
    }
  }

  def getBlob(value: Any, conn: Connection): java.sql.Blob = {
    val serializedValue: Array[Byte] = serialize(value)
    val blob = conn.createBlob()
    blob.setBytes(1, serializedValue)
    blob
  }

  def serialize(value: Any): Array[Byte] = {
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream()
    val os: ObjectOutputStream = new ObjectOutputStream(baos)
    os.writeObject(value)
    os.close()
    baos.toByteArray
  }

  /**
   * Method serializes the passed filters from Spark format to snappy format.
   *
   * @return
   */
  private def serializeFilters: Array[Byte] = {
    if (filters.isDefined) {
      serialize(filters.get)
    } else {
      null
    }
  }

  private def getConnProperties(): ConnectionProperties = {

    // TODO: Check how to make properties Dynamic
    val map: Map[String, String] = HashMap[String, String](("maxActive", "256"),
      ("testOnBorrow", "true"), ("maxIdle", "256"), ("validationInterval", "10000"),
      ("initialSize", "4"), ("driverClassName", "io.snappydata.jdbc.ClientDriver"))

    val poolProperties = new Properties
    poolProperties.setProperty("driver", "io.snappydata.jdbc.ClientDriver")
    poolProperties.setProperty("route-query", "false")

    val executorConnProps = new Properties
    executorConnProps.setProperty("lob-chunk-size", "33554432")
    executorConnProps.setProperty("driver", "io.snappydata.jdbc.ClientDriver")
    executorConnProps.setProperty("route-query", "false")
    executorConnProps.setProperty("lob-direct-buffers", "true")

    ConnectionProperties(hostList(0)._2,
      "io.snappydata.jdbc.ClientDriver", SnappyStoreClientDialect, map,
      poolProperties, executorConnProps, false)

  }

  private def getConnection(connectionProperties: ConnectionProperties,
      hostList: ArrayBuffer[(String, String)]): Connection = {
    useLocatorURL = useLocatorUrl(hostList)
    createConnection(connectionProperties, hostList)
  }

  private def createConnection(connProperties: ConnectionProperties,
      hostList: ArrayBuffer[(String, String)]): Connection = {
    // TODO CHECK THIS SOCKETCREATOR DEPENDENCY RESOLUTION
    // val localhost = SocketCreator.getLocalHost
    var index = -1

    val jdbcUrl = if (useLocatorURL) {
      connProperties.url
    } else {
      // TODO NEEDS TO BE HANDLED=
      // if (index < 0) index = hostList.indexWhere(_._1.contains(localhost.getHostAddress))
      if (index < 0) index = Random.nextInt(hostList.size)
      hostList(index)._2
    }

    // TODO : REMOVE THIS AND ADD DEPENDENCY TO ACCESS ClientAttribute.THRIFT_LOB_DIRECT_BUFFERS
    /**
     * Use direct ByteBuffers when reading BLOBs. This will provide higher
     * performance avoiding a copy but caller must take care to free the BLOB
     * after use else cleanup may happen only in a GC cycle which may be delayed
     * due to no particular GC pressure due to direct ByteBuffers.
     */
    val THRIFT_LOB_DIRECT_BUFFERS = "lob-direct-buffers"
    // -

    // enable direct ByteBuffers for best performance
    val executorProps = connProperties.executorConnProps
    // executorProps.setProperty(ClientAttribute.THRIFT_LOB_DIRECT_BUFFERS, "true")
    executorProps.setProperty(THRIFT_LOB_DIRECT_BUFFERS, "true")

    // setup pool properties
    val props = getAllPoolProperties(jdbcUrl, null,
      connProperties.poolProps, connProperties.hikariCP, isEmbedded = false)
    try {
      // use jdbcUrl as the key since a unique pool is required for each server
      ConnectionPool.getPoolConnection(jdbcUrl, SnappyStoreClientDialect, props,
        executorProps, connProperties.hikariCP)
    } catch {
      case sqle: SQLException => if (hostList.size == 1 || useLocatorURL) {
        throw sqle
      } else {
        hostList.remove(index)
        createConnection(connProperties, hostList)
      }
    }
  }

  private def useLocatorUrl(hostList: ArrayBuffer[(String, String)]): Boolean =
    hostList.isEmpty

  private def prepareScan(conn: Connection, txId: String, columnTable: String,
      projection: Array[Int], serializedFilters: Array[Byte], bucketId: Int,
      relDestroyVersion: Int): (PreparedStatement, ResultSet) = {
    val pstmt = conn.prepareStatement("call sys.COLUMN_TABLE_SCAN(?, ?, ?, 0)")
    pstmt.setString(1, columnTable)
    pstmt.setString(2, projection.mkString(","))
    // serialize the filters
    if ((serializedFilters ne null) && serializedFilters.length > 0) {
      val blob = conn.createBlob()
      blob.setBytes(1, serializedFilters)
      pstmt.setBlob(3, blob)
    } else {
      pstmt.setNull(3, java.sql.Types.BLOB)
    }
    pstmt match {
      case clientStmt: ClientPreparedStatement =>
        val bucketSet = Collections.singleton(Int.box(bucketId))
        clientStmt.setLocalExecutionBucketIds(bucketSet, columnTable, true)
        clientStmt.setMetadataVersion(relDestroyVersion)
      // TODO: Transaction Handling temprary commented.
      // clientStmt.setSnapshotTransactionId(txId)
      case _ =>
        pstmt.execute("call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION(" +
            s"'$columnTable', '${bucketId}', $relDestroyVersion)")
        if (txId ne null) {
          pstmt.execute(s"call sys.USE_SNAPSHOT_TXID('$txId')")
        }
    }

    val rs = pstmt.executeQuery()
    (pstmt, rs)
  }

  // TODO:PS:Review Methods from the ExternalStoreUtils.scala - Duplicate entries
  // Start-----------
  private def addProperty(props: mutable.Map[String, String], key: String,
      default: String): Unit = {
    if (!props.contains(key)) props.put(key, default)
  }

  private def defaultMaxEmbeddedPoolSize: String =
    String.valueOf(math.max(256, Runtime.getRuntime.availableProcessors() * 16))

  private def defaultMaxExternalPoolSize: String =
    String.valueOf(math.max(256, Runtime.getRuntime.availableProcessors() * 8))

  private def getAllPoolProperties(url: String, driver: String,
      poolProps: Map[String, String], hikariCP: Boolean,
      isEmbedded: Boolean): Map[String, String] = {
    // setup default pool properties
    val props = new mutable.HashMap[String, String]()
    if (poolProps.nonEmpty) props ++= poolProps
    if (driver != null && !driver.isEmpty) {
      addProperty(props, "driverClassName", driver)
    }
    val defaultMaxPoolSize = if (isEmbedded) defaultMaxEmbeddedPoolSize
    else defaultMaxExternalPoolSize
    if (hikariCP) {
      props.put("jdbcUrl", url)
      addProperty(props, "maximumPoolSize", defaultMaxPoolSize)
      addProperty(props, "minimumIdle", "10")
      addProperty(props, "idleTimeout", "120000")
    } else {
      props.put("url", url)
      addProperty(props, "maxActive", defaultMaxPoolSize)
      addProperty(props, "maxIdle", defaultMaxPoolSize)
      addProperty(props, "initialSize", "4")
      addProperty(props, "testOnBorrow", "true")
      // embedded validation check is cheap
      if (isEmbedded) addProperty(props, "validationInterval", "0")
      else addProperty(props, "validationInterval", "10000")
    }
    props.toMap
  }
  // End-----------
}

object SnappyColumnBatchRDDHelper {

  // TODO:PS:Review: Moved here from the SmartConnectorRDDHelper
  // and replaced all occurances with
  // the V2ColumnBatchDecoderHeloper.snapshotTxIdForRead wise.
  var snapshotTxIdForRead: ThreadLocal[String] = new ThreadLocal[String]
  var snapshotTxIdForWrite: ThreadLocal[String] = new ThreadLocal[String]

}

