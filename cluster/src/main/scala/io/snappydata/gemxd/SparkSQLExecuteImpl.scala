/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.gemxd

import java.io.{CharArrayWriter, DataOutput}
import java.sql.SQLWarning

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.cache.CacheClosedException
import com.gemstone.gemfire.internal.shared.{ClientSharedUtils, Version}
import com.gemstone.gemfire.internal.{ByteArrayDataInput, InternalDataSerializer}
import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.message.LeadNodeExecutorMsg
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.distributed.{GfxdHeapDataOutputStream, SnappyResultHolder}
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet
import com.pivotal.gemfirexd.internal.iapi.types.{DataValueDescriptor, SQLChar}
import com.pivotal.gemfirexd.internal.impl.sql.execute.ValueRow
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import com.pivotal.gemfirexd.internal.snappy.{LeadNodeExecutionContext, SparkSQLExecute}
import io.snappydata.{Constant, Property, QueryHint}

import org.apache.spark.serializer.{KryoSerializerPool, StructTypeSerializer}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{CachedDataFrame, SnappyContext, SnappySession}
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.util.SnappyUtils
import org.apache.spark.{Logging, SparkEnv}

/**
 * Encapsulates a Spark execution for use in query routing from JDBC.
 */
class SparkSQLExecuteImpl(val sql: String,
    val schema: String,
    val ctx: LeadNodeExecutionContext,
    senderVersion: Version,
    pvs: Option[ParameterValueSet]) extends SparkSQLExecute with Logging {

  // spark context will be constructed by now as this will be invoked when
  // DRDA queries will reach the lead node

  if (Thread.currentThread().getContextClassLoader != null) {
    val loader = SnappyUtils.getSnappyStoreContextLoader(
      SparkSQLExecuteImpl.getContextOrCurrentClassLoader)
    Thread.currentThread().setContextClassLoader(loader)
  }

  private[this] val session = SnappySessionPerConnection
      .getSnappySessionForConnection(ctx.getConnId)

  if (ctx.getUserName != null && !ctx.getUserName.isEmpty) {
    session.conf.set(Attribute.USERNAME_ATTR, ctx.getUserName)
    session.conf.set(Attribute.PASSWORD_ATTR, ctx.getAuthToken)
  }

  session.setCurrentSchema(schema)

  session.setPreparedQuery(preparePhase = false, pvs)

  private[this] val df = Utils.sqlInternal(session, sql)

  private[this] val thresholdListener = Misc.getMemStore.thresholdListener()

  private[this] val hdos = new GfxdHeapDataOutputStream(
    thresholdListener, sql, false, senderVersion)

  private[this] val querySchema = df.schema

  private[this] lazy val colTypes = getColumnTypes

  // check for query hint to serialize complex types as JSON strings
  private[this] val complexTypeAsJson = SparkSQLExecuteImpl.getJsonProperties(session)

  private val (allAsClob, columnsAsClob) = SparkSQLExecuteImpl.getClobProperties(session)

  override def packRows(msg: LeadNodeExecutorMsg,
      snappyResultHolder: SnappyResultHolder): Unit = {

    var srh = snappyResultHolder
    val isLocalExecution = msg.isLocallyExecuted

    val bm = SparkEnv.get.blockManager
    val rddId = df.rddId
    var blockReadSuccess = false
    try {
      // get the results and put those in block manager to avoid going OOM
      // TODO: can optimize to ship immediately if plan is not ordered
      // TODO: can ship CollectAggregateExec processing to the server node
      // which is supported via the "skipLocalCollectProcessing" flag to the
      // call below (but that has additional overheads of plan
      //   shipping/compilation etc and lack of proper BlockManager usage in
      //   messaging + server-side final processing, so do it selectively)
      val partitionBlocks = df.collectWithHandler(CachedDataFrame,
        CachedDataFrame.localBlockStoreResultHandler(rddId, bm),
        CachedDataFrame.localBlockStoreDecoder(querySchema.length, bm))
      hdos.clearForReuse()
      SparkSQLExecuteImpl.writeMetaData(srh, hdos, tableNames, nullability, getColumnNames,
        colTypes, getColumnDataTypes, session.getWarnings)

      var id = 0
      for (block <- partitionBlocks) {
        block match {
          case null => // skip but still id has to be incremented
          case data: Array[Byte] => if (data.length > 0) {
            hdos.write(data)
          }
          case p: RDDBlockId =>
            val partitionData = Utils.getPartitionData(p, bm)
            // remove the block once a local handle to it has been obtained
            bm.removeBlock(p, tellMaster = false)
            hdos.write(partitionData)
        }
        logTrace(s"Writing data for partition ID = $id: $block")
        val dosSize = hdos.size()
        if (dosSize > GemFireXDUtils.DML_MAX_CHUNK_SIZE) {
          if (isLocalExecution) {
            // prepare SnappyResultHolder with all data and create new one
            SparkSQLExecuteImpl.handleLocalExecution(srh, hdos)
            msg.sendResult(srh)
            srh = new SnappyResultHolder(this, msg.isUpdateOrDeleteOrPut)
          } else {
            // throttle sending if target node is CRITICAL_UP
            val targetMember = msg.getSender
            if (thresholdListener.isCritical ||
                thresholdListener.isCriticalUp(targetMember)) {
              try {
                var throttle = true
                for (_ <- 1 to 5 if throttle) {
                  Thread.sleep(4)
                  throttle = thresholdListener.isCritical ||
                      thresholdListener.isCriticalUp(targetMember)
                }
              } catch {
                case ie: InterruptedException => Misc.checkIfCacheClosing(ie)
              }
            }

            msg.sendResult(srh)
            // clear the metadata flag for subsequent chunks
            srh.clearHasMetadata()
          }
          logTrace(s"Sent one batch for result, current partition ID = $id")
          hdos.clearForReuse()
          // 0/1 indicator is now written in serializeRows itself to allow
          // ByteBuffer to be passed as is in the chunks list of
          // GfxdHeapDataOutputStream and avoid a copy
        }
        id += 1
      }
      blockReadSuccess = true

      if (isLocalExecution) {
        SparkSQLExecuteImpl.handleLocalExecution(srh, hdos)
      }
      msg.lastResult(srh)

    } finally {
      if (!blockReadSuccess) {
        // remove any cached results from block manager
        bm.removeRdd(rddId)
      }
    }
  }

  override def serializeRows(out: DataOutput, hasMetadata: Boolean): Unit =
    SparkSQLExecuteImpl.serializeRows(out, hasMetadata, hdos)

  private lazy val (tableNames, nullability) = {
    val analyzed = df.queryExecution.analyzed
    val relations = analyzed.collectLeaves
        .filter(_.isInstanceOf[LogicalRelation])
        .map(_.asInstanceOf[LogicalRelation])

    def getQualifier(a: AttributeReference): Option[String] = {
      relations.foreach { relation =>
        if (relation.output.map(_.exprId.id).contains(a.exprId.id)) {
          return Some(Seq(relation.catalogTable.get.identifier.database.getOrElse(""),
              relation.catalogTable.get.identifier.table).mkString("."))
        }
      }
      None
    }

    val attributes = analyzed match {
      case Project(fields, child) =>
        fields.map { projExp =>
          val attributes = projExp.collectLeaves().filter(_.isInstanceOf[AttributeReference])
              .map(_.asInstanceOf[AttributeReference])
          // for 'SELECT 1...', 'SELECT col1...', 'SELECT col1 * col2...' queries
          // attributes size will be 0, 1, 2 respectively
          if (attributes.size > 0 && getQualifier(attributes.head).isDefined) {
            // here 1st attribute is considered. Need to check if this behaviour is ok
            projExp.toAttribute.withQualifier(getQualifier(attributes.head))
          } else {
            projExp.toAttribute
          }
        }
      case _ => analyzed.output
    }

    SparkSQLExecuteImpl.
        getTableNamesAndNullability(session, attributes)
  }

  def getColumnNames: Array[String] = {
    querySchema.fieldNames
  }

  private def getColumnTypes: Array[(Int, Int, Int)] =
    querySchema.map(f => {
      SparkSQLExecuteImpl.getSQLType(f.dataType, complexTypeAsJson,
        f.metadata, Utils.toLowerCase(f.name), allAsClob, columnsAsClob)
    }).toArray

  private def getColumnDataTypes: Array[DataType] =
    querySchema.map(_.dataType).toArray
}

object SparkSQLExecuteImpl {

  def getJsonProperties(session: SnappySession): Boolean = session.getPreviousQueryHints.get(
    QueryHint.ComplexTypeAsJson.toString) match {
    case null => Constant.COMPLEX_TYPE_AS_JSON_DEFAULT
    case v => ClientSharedUtils.parseBoolean(v)
  }

  def getClobProperties(session: SnappySession): (Boolean, Set[String]) =
    session.getPreviousQueryHints.get(QueryHint.ColumnsAsClob.toString) match {
    case null => (false, Set.empty[String])
    case v => Utils.parseColumnsAsClob(v, session)
  }

  def getSQLType(dataType: DataType, complexTypeAsJson: Boolean,
      metaData: Metadata = Metadata.empty, metaName: String = "",
      allAsClob: Boolean = false, columnsAsClob: Set[String] = Set.empty): (Int, Int, Int) = {
    dataType match {
      case IntegerType => (StoredFormatIds.SQL_INTEGER_ID, -1, -1)
      case StringType =>
        TypeUtilities.getMetadata[String](Constant.CHAR_TYPE_BASE_PROP, metaData) match {
          case Some(base) =>
            lazy val size = TypeUtilities.getMetadata[Long](
              Constant.CHAR_TYPE_SIZE_PROP, metaData)
            base match {
              case "CHAR" =>
                val charSize = size match {
                  case Some(s) => s.toInt
                  case None => Constant.MAX_CHAR_SIZE
                }
                (StoredFormatIds.SQL_CHAR_ID, charSize, -1)
              case "STRING" if allAsClob ||
                  (columnsAsClob.nonEmpty && columnsAsClob.contains(metaName)) =>
                (StoredFormatIds.SQL_CLOB_ID, -1, -1)
              case "CLOB" => (StoredFormatIds.SQL_CLOB_ID, -1, -1)
              case _ =>
                val varcharSize = size match {
                  case Some(s) => s.toInt
                  case None => Constant.MAX_VARCHAR_SIZE
                }
                (StoredFormatIds.SQL_VARCHAR_ID, varcharSize, -1)
            }
          case None => if (allAsClob ||
              (columnsAsClob.nonEmpty && columnsAsClob.contains(metaName))) {
            (StoredFormatIds.SQL_CLOB_ID, -1, -1)
          } else {
            // check if size is specified
            val size = TypeUtilities.getMetadata[Long](
              Constant.CHAR_TYPE_SIZE_PROP, metaData) match {
              case Some(s) => s.toInt
              case None => Constant.MAX_VARCHAR_SIZE
            }
            (StoredFormatIds.SQL_VARCHAR_ID, size, -1)
          }
        }
      case LongType => (StoredFormatIds.SQL_LONGINT_ID, -1, -1)
      case TimestampType => (StoredFormatIds.SQL_TIMESTAMP_ID, -1, -1)
      case DateType => (StoredFormatIds.SQL_DATE_ID, -1, -1)
      case DoubleType => (StoredFormatIds.SQL_DOUBLE_ID, -1, -1)
      case t: DecimalType => (StoredFormatIds.SQL_DECIMAL_ID,
          t.precision, t.scale)
      case FloatType => (StoredFormatIds.SQL_REAL_ID, -1, -1)
      case BooleanType => (StoredFormatIds.SQL_BOOLEAN_ID, -1, -1)
      case ShortType => (StoredFormatIds.SQL_SMALLINT_ID, -1, -1)
      case ByteType => (StoredFormatIds.SQL_TINYINT_ID, -1, -1)
      case BinaryType => (StoredFormatIds.SQL_BLOB_ID, -1, -1)
      case _: ArrayType | _: MapType | _: StructType =>
        // indicates complex types serialized as strings
        if (complexTypeAsJson) (StoredFormatIds.REF_TYPE_ID, -1, -1)
        else (StoredFormatIds.SQL_BLOB_ID, -1, -1)

      // send across rest as objects that will be displayed as strings
      case _ => (StoredFormatIds.REF_TYPE_ID, -1, -1)
    }
  }

  def getTableNamesAndNullability(session: SnappySession,
      output: Seq[expressions.Attribute]): (Seq[String], Seq[Boolean]) = {
    output.map { a =>
      val fn = a.qualifiedName
      val dotIdx = fn.lastIndexOf('.')
      if (dotIdx > 0) {
        val tableName = fn.substring(0, dotIdx)
        val fullTableName = if (tableName.indexOf('.') > 0) tableName
        else session.getCurrentSchema + '.' + tableName
        (fullTableName, a.nullable)
      } else {
        ("", a.nullable)
      }
    }.unzip
  }

  def writeMetaData(srh: SnappyResultHolder, hdos: GfxdHeapDataOutputStream,
      tableNames: Seq[String], nullability: Seq[Boolean], columnNames: Array[String],
      colTypes: Array[(Int, Int, Int)], dataTypes: Array[DataType],
      warnings: SQLWarning): Unit = {
    // indicates that the metadata is being packed too
    srh.setHasMetadata()
    DataSerializer.writeStringArray(tableNames.toArray, hdos)
    DataSerializer.writeStringArray(columnNames, hdos)
    DataSerializer.writeBooleanArray(nullability.toArray, hdos)
    var i = 0
    while (i < colTypes.length) {
      val (tp, precision, scale) = colTypes(i)
      InternalDataSerializer.writeSignedVL(tp, hdos)
      tp match {
        case StoredFormatIds.SQL_DECIMAL_ID =>
          InternalDataSerializer.writeSignedVL(precision, hdos) // precision
          InternalDataSerializer.writeSignedVL(scale, hdos) // scale
        case StoredFormatIds.SQL_VARCHAR_ID |
             StoredFormatIds.SQL_CHAR_ID =>
          // Write the size as precision
          InternalDataSerializer.writeSignedVL(precision, hdos)
        case StoredFormatIds.REF_TYPE_ID =>
          // Write the DataType
          hdos.write(KryoSerializerPool.serialize((kryo, out) =>
            StructTypeSerializer.writeType(kryo, out, dataTypes(i))))
        case _ => // ignore for others
      }
      i += 1
    }
    DataSerializer.writeObject(warnings, hdos)
  }

  def getContextOrCurrentClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader)
        .getOrElse(getClass.getClassLoader)

  def handleLocalExecution(srh: SnappyResultHolder,
      hdos: GfxdHeapDataOutputStream): Unit = {
    val size = hdos.size()
    // prepare SnappyResultHolder with all data and create new one
    if (size > 0) {
      val bytes = new Array[Byte](size + 1)
      // byte 1 will indicate that the metainfo is being packed too
      bytes(0) = if (srh.hasMetadata) 0x1 else 0x0
      hdos.sendTo(bytes, 1)
      srh.fromSerializedData(bytes, bytes.length, null)
    }
  }

  def serializeRows(out: DataOutput, hasMetadata: Boolean,
      hdos: GfxdHeapDataOutputStream): Unit = {
    val numBytes = hdos.size
    if (numBytes > 0) {
      InternalDataSerializer.writeArrayLength(numBytes + 1, out)
      // byte 1 will indicate that the metainfo is being packed too
      out.writeByte(if (hasMetadata) 0x1 else 0x0)
      hdos.sendTo(out)
    } else {
      InternalDataSerializer.writeArrayLength(0, out)
    }
  }

  lazy val STRING_AS_CLOB: Boolean = System.getProperty(
    Constant.STRING_AS_CLOB_PROP, "false").toBoolean

  def getRowIterator(dvds: Array[DataValueDescriptor], types: Array[Int],
      precisions: Array[Int], scales: Array[Int], dataTypes: Array[AnyRef],
      input: ByteArrayDataInput): java.util.Iterator[ValueRow] = {
    // initialize JSON generators if required
    var writers: ArrayBuffer[CharArrayWriter] = null
    var generators: ArrayBuffer[AnyRef] = null
    for (d <- dataTypes) {
      if (d ne null) {
        if (writers eq null) {
          writers = new ArrayBuffer[CharArrayWriter](2)
          generators = new ArrayBuffer[AnyRef](2)
        }
        val size = writers.length
        val writer = new CharArrayWriter()
        writers += writer
        generators += Utils.getJsonGenerator(d.asInstanceOf[DataType],
          s"col_$size", writer)
      }
    }
    val execRow = new ValueRow(dvds)
    val numFields = types.length
    val unsafeRows = CachedDataFrame.decodeUnsafeRows(numFields,
      input.array(), input.position(), input.available())
    unsafeRows.map { row =>
      var index = 0
      var writeIndex = 0
      while (index < numFields) {
        val dvd = dvds(index)
        if (row.isNullAt(index)) {
          dvd.setToNull()
          index += 1
        } else {
          types(index) match {
            case StoredFormatIds.SQL_VARCHAR_ID |
                 StoredFormatIds.SQL_CLOB_ID =>
              val utf8String = row.getUTF8String(index)
              dvd.setValue(utf8String.toString)

            case StoredFormatIds.SQL_CHAR_ID =>
              val precision = precisions(index)
              val utf8String = row.getUTF8String(index)
              var fixedString = utf8String.toString
              val stringLen = fixedString.length
              if (stringLen != precision) {
                if (stringLen < precision) {
                  // add blank padding
                  val sb = new java.lang.StringBuilder(precision)
                  val blanks = new Array[Char](precision - stringLen)
                  SQLChar.appendBlanks(blanks, 0, blanks.length)
                  fixedString = sb.append(fixedString).append(blanks).toString
                } else {
                  // truncate
                  fixedString = fixedString.substring(0, precision)
                }
              }
              dvd.setValue(fixedString)

            case StoredFormatIds.SQL_INTEGER_ID =>
              dvd.setValue(row.getInt(index))
            case StoredFormatIds.SQL_LONGINT_ID =>
              dvd.setValue(row.getLong(index))
            case StoredFormatIds.SQL_SMALLINT_ID =>
              dvd.setValue(row.getShort(index))

            case StoredFormatIds.SQL_TIMESTAMP_ID =>
              val ts = DateTimeUtils.toJavaTimestamp(row.getLong(index))
              dvd.setValue(ts)
            case StoredFormatIds.SQL_DECIMAL_ID =>
              val dec = row.getDecimal(index, precisions(index), scales(index))
              dvd.setBigDecimal(dec.toJavaBigDecimal)
            case StoredFormatIds.SQL_DATE_ID =>
              val dt = DateTimeUtils.toJavaDate(row.getInt(index))
              dvd.setValue(dt)
            case StoredFormatIds.SQL_BOOLEAN_ID =>
              dvd.setValue(row.getBoolean(index))
            case StoredFormatIds.SQL_TINYINT_ID =>
              dvd.setValue(row.getByte(index))
            case StoredFormatIds.SQL_REAL_ID =>
              dvd.setValue(row.getFloat(index))
            case StoredFormatIds.SQL_DOUBLE_ID =>
              dvd.setValue(row.getDouble(index))
            case StoredFormatIds.REF_TYPE_ID =>
              // convert to Json using JacksonGenerator
              val writer = writers(writeIndex)
              val generator = generators(writeIndex)
              Utils.generateJson(generator, row, index,
                dataTypes(index).asInstanceOf[DataType])
              val json = writer.toString
              writer.reset()
              dvd.setValue(json)
              writeIndex += 1
            case StoredFormatIds.SQL_BLOB_ID =>
              // all complex types too work with below because all of
              // Array, Map, Struct (as well as Binary itself) transport
              // data in the same way in UnsafeRow (offsetAndWidth)
              dvd.setValue(row.getBinary(index))
            case other => throw new GemFireXDRuntimeException(
              s"SparkSQLExecuteImpl: unexpected typeFormatId $other")
          }
          index += 1
        }
      }
      if ((generators ne null) && !unsafeRows.hasNext) {
        generators.foreach(Utils.closeJsonGenerator)
      }

      execRow
    }.asJava
  }
}

object SnappySessionPerConnection {

  private val connectionIdMap =
    new java.util.concurrent.ConcurrentHashMap[java.lang.Long, SnappySession]()

  def getSnappySessionForConnection(connId: Long): SnappySession = {
    val connectionID = Long.box(connId)
    val session = connectionIdMap.get(connectionID)
    if (session != null) session
    else {
      val session = SnappyContext.globalSparkContext match {
        // use a CancelException to force failover by client to another lead if available
        case null => throw new CacheClosedException("No SparkContext ...")
        case sc => new SnappySession(sc)
      }
      Property.PlanCaching.set(session.sessionState.conf, true)
      val oldSession = connectionIdMap.putIfAbsent(connectionID, session)
      if (oldSession == null) session else oldSession
    }
  }

  def getAllSessions: Seq[SnappySession] = connectionIdMap.values().asScala.toSeq

  def removeSnappySession(connectionID: java.lang.Long): Unit = {
    connectionIdMap.remove(connectionID)
  }
}
