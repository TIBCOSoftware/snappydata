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
package org.apache.spark.sql.execution.oplog.impl


import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.internal.cache._
import com.pivotal.gemfirexd.internal.catalog.UUID
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.store._
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.{ColumnDescriptor, ColumnDescriptorList}
import com.pivotal.gemfirexd.internal.iapi.types.{DataType => _, _}

import org.apache.spark.sql.{Row, SnappySession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.collection.MultiBucketExecutorPartition
import org.apache.spark.sql.execution.RDDKryo
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, TaskContext}
import scala.annotation.meta.param
import scala.collection.mutable

import com.gemstone.gemfire.internal.shared.FetchRequest
import com.pivotal.gemfirexd.internal.client.am.Types
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import io.snappydata.recovery.RecoveryService
import io.snappydata.recovery.RecoveryService.mostRecentMemberObject
import io.snappydata.thrift.CatalogTableObject
import org.apache.avro.generic.GenericData

import org.apache.spark.serializer.StructTypeSerializer
import org.apache.spark.sql.execution.columnar.encoding.{ColumnDecoder, ColumnEncoding, ColumnStatsSchema}
import org.apache.spark.sql.execution.columnar.impl.{ColumnFormatKey, ColumnFormatValue}

class OpLogColumnRdd(
    @transient private val session: SnappySession,
    private var dbTableName: String,
    private var tblName: String,
    private var sch: StructType,
    private var projection: Array[Int],
    @transient private[sql] val filters: Array[Expression],
    private[sql] var fullScan: Boolean,
    @(transient@param) partitionPruner: () => Int)
    extends RDDKryo[Any](session.sparkContext, Nil) with KryoSerializable {
  var rowFormatter: RowFormatter = null
  var result: Seq[Row] = null

  /**
   * Method gets DataValueDescritor type from given StructField
   *
   * @param dataType
   * @param metadata
   * @param isNullable
   * @return
   */
  def getDVDType(field: StructField): DataTypeDescriptor = {
    val dataType = field.dataType
    val isNullable = field.nullable
    val metadata = field.metadata

    dataType match {
      case LongType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT, isNullable)
      case IntegerType => DataTypeDescriptor.INTEGER
      case BooleanType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN, isNullable)
      case ByteType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.SMALLINT, isNullable)
      case FloatType => DataTypeDescriptor.getSQLDataTypeDescriptor("float", isNullable)
      case BinaryType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BLOB, isNullable)
      case DoubleType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE, isNullable)
      case ShortType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.SMALLINT, isNullable)
      case TimestampType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP,
        isNullable)
      case DateType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DATE, isNullable)
      case d: DecimalType => {
        val precision = d.precision
        val scale = d.scale
        new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.DECIMAL),
          precision, scale, isNullable, precision)
      }
      case StringType => {
        if (metadata.contains("base")) {
          metadata.getString("base") match {
            case "STRING" => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CLOB, isNullable)
            case "VARCHAR" =>
              DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, isNullable,
                metadata.getLong("size").toInt)
            case "CLOB" => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CLOB, isNullable)
          }
        }
        else {
          throw new Exception(s"Error getting proper DataTypeDescriptor for DataType: $dataType")
        }
      }
      case _ => new DataTypeDescriptor(TypeId.CHAR_ID, true)
    }
  }

  def getRowFormatter: RowFormatter = {
    val cdl = new ColumnDescriptorList()
    sch.toList.foreach(field => {
      val cd = new ColumnDescriptor(
        field.name,
        sch.fieldIndex(field.name) + 1,
        getDVDType(field),
        // getDVDType(field.dataType),
        null,
        null,
        null.asInstanceOf[UUID],
        null.asInstanceOf[UUID],
        0L,
        0L,
        0L,
        false
      )
      cdl.add(null, cd)
    })
    // if (RecoveryService.getProvider(dbTableName).equalsIgnoreCase("COLUMN")) {
    cdl.add(null, new ColumnDescriptor("SNAPPYDATA_INTERNAL_ROWID", sch.size + 1,
      DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT, false),
      null,
      null,
      null.asInstanceOf[UUID],
      null.asInstanceOf[UUID],
      0L,
      0L,
      0L,
      false))
    // }
    logInfo(s"KN: columndescriptor list = ${cdl}")
    val schemaName = tblName.split("\\.")(0)
    val tableName = tblName.split("\\.")(1)
    val rf = new RowFormatter(cdl, schemaName, tableName, 0, null, false)
    logInfo(s"1891: rowformatter = ${rf}")
    rf
  }

  private def fillRowUsingByteArrayArray(
      row: AbstractCompactExecRow,
      value: Array[Array[Byte]]): Unit = {
    if (rowFormatter.hasLobs) {
      row.setRowArray(value, rowFormatter)
    } else {
      row.setRowArray(value(0), rowFormatter)
    }
  }

  def getPlaceHolderDiskRegion(
      diskStr: DiskStoreImpl,
      regPath: String): PlaceHolderDiskRegion = {
    var phdr: PlaceHolderDiskRegion = null
    val iter = diskStr.getAllDiskRegions.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val adr = entry.getValue()
      val adrPath = adr.getFullPath
      if (PartitionedRegionHelper.unescapePRPath(adrPath).equals(regPath) && adr.isBucket) {
        phdr = adr.asInstanceOf[PlaceHolderDiskRegion]
      } else {
        logInfo(s"1891: getPlaceHolderDiskRegion ${PartitionedRegionHelper.unescapePRPath(adrPath)} -- ${regPath} -- ${adr.isBucket}")
      }
    }
    phdr
  }

  def readRowData(phdrRow: PlaceHolderDiskRegion): Unit = {
    val regMapItr = phdrRow.getRegionMap.regionEntries().iterator()
    while (regMapItr.hasNext) {
      logInfo("1891: regmapitr has more entries")
      val regEntry = regMapItr.next()
      val value = DiskEntry.Helper.readValueFromDisk(
        regEntry.asInstanceOf[DiskEntry], phdrRow)
      if (value != Token.TOMBSTONE) {
        val dvdArr = new Array[DataValueDescriptor](sch.length)
        if (value.isInstanceOf[Array[Array[Byte]]]) {
          val valueArr = value.asInstanceOf[Array[Array[Byte]]]
          rowFormatter.getColumns(valueArr, dvdArr, (1 to sch.size).toArray)
          val row = Row.fromSeq(dvdArr.zipWithIndex.map { case (dvd, i) => {
            val field = sch(i)
            logInfo(s"1891: at index ${i} datatype is ${field.dataType} ")
            field.dataType match {
              case ShortType =>
                logInfo(s"1891: for index ${i} small int $dvd")
                new Integer(33).shortValue()
              case ByteType =>
                logInfo(s"1891: for index ${i} byte $dvd")
                dvd.getByte
              case _ =>
                logInfo(s"1891: for index ${i} object $dvd")
                dvd.getObject
            }
          }
          })
          logInfo(s"1891: rowasseq[][]: ${row}")
          result = (result :+ row)
        } else {
          val valueArr = value.asInstanceOf[Array[Byte]]
          rowFormatter.getColumns(valueArr, dvdArr, (1 to sch.size).toArray)
          // dvd gets gemfire data types
          val row = Row.fromSeq(dvdArr.zipWithIndex.map { case (dvd, i) => {
            val field = sch(i)
            logInfo(s"1891: at index ${i} datatype is ${field.dataType} ")
            field.dataType match {
              case ShortType =>
                logInfo(s"1891: for index ${i} small int $dvd")
                dvd.getShort
              case ByteType =>
                logInfo(s"1891: for index ${i} byte $dvd")
                dvd.getByte
              case _ =>
                logInfo(s"1891: for index ${i} object $dvd")
                dvd.getObject
            }
          }
          })
          logInfo(s"1891: rowasseq[]: ${row}")
          result = (result :+ row)
        }
      }
    }
  }

  def readColData(phdrCol: PlaceHolderDiskRegion): Unit = {
    val colRegEntriesItr = phdrCol.getRegionMap.regionEntries().iterator()
    var numOfRows = -1
    var tbl: Array[Array[Any]] = null

    while (colRegEntriesItr.hasNext) {
      logInfo("1891: column region has more entries")
      val regEntry = colRegEntriesItr.next()
      val value = DiskEntry.Helper.readValueFromDisk(regEntry.asInstanceOf[DiskEntry], phdrCol)
      val numStatsColumns = sch.size * ColumnStatsSchema.NUM_STATS_PER_COLUMN + 1

      val scanColBuf = org.apache.spark.sql.collection.SharedUtils
          .toUnsafeRow(value.asInstanceOf[ColumnFormatValue].getBuffer, numStatsColumns)
      val colNum = regEntry.getKey.asInstanceOf[ColumnFormatKey].getColumnIndex
      if (colNum == -1 || colNum < 0) { // stats buffer
        logInfo(s"1891: reading stats colNum = ${colNum}")
        // TODO check if ordinal 0 can be non hardcoded
        numOfRows = scanColBuf.getInt(0)
        logInfo(s"1891: number of rows = ${numOfRows}")
        if (tbl != null && !tbl.equals(Array.ofDim[Any](numOfRows, sch.length))) {
          logInfo("1891: adding prev col batch to result")
          tbl.foreach(arr => result = result :+ Row.fromSeq(arr.toSeq))
        }
        tbl = Array.ofDim[Any](numOfRows, sch.length)
      } else if (colNum >= 0) {
        logInfo(s"1891: reading col batches colNum = ${colNum}")
        var valueBuffer = value.asInstanceOf[ColumnFormatValue]
            .getValueRetain(FetchRequest.DECOMPRESS).getBuffer
        // -3 for delete at columnbatch
        logInfo(s"1891: getting decoder for col num${colNum - 1}")
        val decoder = ColumnEncoding.getColumnDecoder(valueBuffer, sch(colNum - 1))
        val valueArray = if (valueBuffer == null || valueBuffer.isDirect) {
          logInfo(s"1891: valueBuffers is direct : ${valueBuffer.isDirect}")
          null
        } else {
          valueBuffer.array()
        }
        (0 until numOfRows).map(rowNum => {
          logInfo(s"1891: adding prev col batch to tbl rowNum = ${rowNum}")
          // tbl(rowNum)(colNum - 1) = decoder.readInt(valueArray, rowNum)
          tbl(rowNum)(colNum - 1) = getDecodedValue(
            decoder, valueArray, sch(colNum - 1).dataType, rowNum)
        })
      }
    }
    if (tbl != null && !tbl.equals(Array.ofDim[Any](numOfRows, sch.length))) {
      logInfo("1891: adding prev col batch to result")
      tbl.foreach(arr => result = result :+ Row.fromSeq(arr.toSeq))
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Any] = {
    logInfo("1891: started compute()", new Throwable)
    rowFormatter = getRowFormatter
    // TODO: hmeka think of better way to build iterator
    result = Seq.empty
    val diskStrs = Misc.getGemFireCache.listDiskStores()
    var diskStrCol: DiskStoreImpl = null
    var diskStrRow: DiskStoreImpl = null

    import scala.collection.JavaConversions._
    // TODO: hmeka to figure out to get diskstore name
    for (d <- diskStrs) {
      logInfo(s"1891: diskstore name = ${d.getName}")
      if (d.getName.equals("GFXD-DEFAULT-DISKSTORE")) {
        diskStrCol = d
      } else if (d.getName.equals("SNAPPY-INTERNAL-DELTA")) {
        diskStrRow = d
      }
    }

    assert(diskStrCol != null && diskStrRow != null, s"1891: col/row disk store is null")

    val colRegPath = if (Misc.getRegionPath(tblName) == null) {
      "null"
    } else {
      val s = Misc.getRegionPath(tblName)
      logInfo(s"1891: split index = ${split.index}")
      s"/_PR//B_${s.substring(1, s.length - 1)}/_${split.index}"
    }
    logInfo(s"1891: col regName get is ${colRegPath}")

    val rowRegPath = s"/_PR//B_${dbTableName.replace('.', '/').toUpperCase()}/${split.index}"
    logInfo(s"1891: row regName get is ${rowRegPath}")

    val phdrCol = getPlaceHolderDiskRegion(diskStrCol, colRegPath)
    val phdrRow = getPlaceHolderDiskRegion(diskStrRow, rowRegPath)
    logInfo(s"1891: phdrcolname = ${phdrCol == null} and ${phdrRow == null}")

    readRowData(phdrRow)
    readColData(phdrCol)
    logInfo(s"1891: split number from compute is ${split.index} and result size is ${result.size}")
    result.iterator
  }

  def getDecodedValue(
      decoder: ColumnDecoder,
      value: Array[Byte],
      dataType: DataType,
      rowNum: Integer): Any = {
    dataType match {
      case LongType => decoder.readLong(value, rowNum)
      case IntegerType => decoder.readInt(value, rowNum)
      case BooleanType => decoder.readBoolean(value, rowNum)
      case ByteType => decoder.readByte(value, rowNum)
      case FloatType => decoder.readFloat(value, rowNum)
      case DoubleType => decoder.readDouble(value, rowNum)
      case BinaryType => decoder.readBinary(value, rowNum)
      case ShortType => decoder.readShort(value, rowNum)
      case TimestampType => decoder.readTimestamp(value, rowNum)
      case StringType => decoder.readUTF8String(value, rowNum)
      case DateType => decoder.readDate(value, rowNum)
      case _ => null
    }
  }

  def getNumBuckets(schemaName: String, tableName: String): Integer = {
    val catalogObjects = mostRecentMemberObject.getCatalogObjects
    logInfo(s"1891: catalogobjects from getPartitions = ${catalogObjects}")
    var numBuckets: Integer = null
    catalogObjects.toArray.foreach(catObj =>
      if (numBuckets == null) {
        numBuckets = catObj match {
          case c: CatalogTableObject =>
            if (c.schemaName.equals(schemaName) && c.tableName.equals(tableName)) {
              val numBucketsStr = c.storage.properties.get("buckets")
              assert(numBucketsStr != null, "property 'buckets' not found in CatalogTableObject")
              Integer.parseInt(numBucketsStr)
            } else null
          case _ => null
        }
      }
    )
    numBuckets
  }

  // private[this] var allPartitions: Array[Partition] = _
  def getPartitionEvaluator: () => Array[Partition] = () => getPartitions

  override protected def getPartitions: Array[Partition] = {
    val schemaName = dbTableName.split('.')(0)
    val tableName = dbTableName.split('.')(1)
    val numBuckets = getNumBuckets(schemaName, tableName)
    logInfo(s"1891: numbuckets = ${numBuckets}")
    val parr = (0 until numBuckets).map { p =>
      val prefNodes = RecoveryService.getExecutorHost(dbTableName, p)
      logInfo(s"1891: prefNodes = ${prefNodes}")
      val buckets = new mutable.ArrayBuffer[Int](numBuckets)
      buckets += p
      new MultiBucketExecutorPartition(p, buckets, numBuckets, prefNodes)
    }.toArray[Partition]
    parr
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    super.write(kryo, output)
    output.writeString(tblName)
    output.writeString(dbTableName)
    StructTypeSerializer.write(kryo, output, sch)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)
    tblName = input.readString()
    dbTableName = input.readString()
    sch = StructTypeSerializer.read(kryo, input, c = null)
    logWarning(s"1891: sch = ${sch}")
  }
}