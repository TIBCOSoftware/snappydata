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


import java.sql.Timestamp

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
import org.apache.spark.sql.execution.RDDKryo
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, TaskContext}
import scala.annotation.meta.param
import scala.collection.mutable

import com.gemstone.gemfire.internal.shared.FetchRequest
import com.pivotal.gemfirexd.internal.client.am.Types
import io.snappydata.recovery.RecoveryService
import io.snappydata.recovery.RecoveryService.mostRecentMemberObject
import io.snappydata.thrift.CatalogTableObject

import org.apache.spark.serializer.StructTypeSerializer
import org.apache.spark.sql.execution.columnar.encoding.{ColumnDecoder, ColumnEncoding, ColumnStatsSchema}
import org.apache.spark.sql.execution.columnar.impl.{ColumnFormatKey, ColumnFormatValue}

class OpLogRdd(
    @transient private val session: SnappySession,
    private var dbTableName: String,
    private var tblName: String,
    private var sch: StructType,
    private var provider: String,
    private var projection: Array[Int],
    @transient private[sql] val filters: Array[Expression],
    private[sql] var fullScan: Boolean,
    @(transient@param) partitionPruner: () => Int)
    extends RDDKryo[Any](session.sparkContext, Nil) with KryoSerializable {

  /**
   * Method gets DataValueDescritor type from given StructField
   *
   * @param field StructField of a column
   * @return DataTypeDescriptor
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

  /**
   * Method creates and returns a RowFormatter from schema
   *
   * @return RowFormatter
   */
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

  /**
   * Method gets PlaceHolderDiskRegion type from given region path
   *
   * @param diskStr diskstore in which region exists
   * @param regPath region path constructed from given table name
   * @return PlaceHolderDiskRegion
   */
  def getPlaceHolderDiskRegion(
      diskStr: DiskStoreImpl,
      regPath: String): PlaceHolderDiskRegion = {
    logDebug(s"Getting PlaceHolderDiskRegion for Disk Store $diskStr for Region Path $regPath")
    var phdr: PlaceHolderDiskRegion = null
    val iter = diskStr.getAllDiskRegions.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val adr = entry.getValue()
      val adrPath = adr.getFullPath
      logInfo(s"PP: getPlaceHolderDiskRegion adrpath: $adrPath \n adr: ${adr.toString}" +
          s" is adr bucket : ${adr.isBucket} ")
      if (PartitionedRegionHelper.unescapePRPath(adrPath).equals(regPath) && adr.isBucket) {
        phdr = adr.asInstanceOf[PlaceHolderDiskRegion]
        logInfo(s"PP: getPlaceHolderDiskRegion:" +
            s" ${PartitionedRegionHelper.unescapePRPath(adrPath)} -- ${regPath} -- ${adr.isBucket}")
      } else {
        logInfo(s"1891: getPlaceHolderDiskRegion " +
            s"${PartitionedRegionHelper.unescapePRPath(adrPath)} -- ${regPath} -- ${adr.isBucket}")
      }
    }
    phdr
  }

  /**
   * Reads data from row buffer regions and appends result to provided ArrayBuffer
   *
   * @param phdrRow PlaceHolderDiskRegion of row
   * @param result  ArrayBuffer to which the rows read from phdrRow are appended
   */
  def readRowData(phdrRow: PlaceHolderDiskRegion, result: mutable.ArrayBuffer[Row]): Unit = {
    val rowFormatter = getRowFormatter
    val dvdArr = new Array[DataValueDescriptor](sch.length)
    val regMapItr = phdrRow.getRegionMap.regionEntries().iterator()
    while (regMapItr.hasNext) {
      logInfo("1891: regmapitr has more entries")
      val regEntry = regMapItr.next()
      val value = DiskEntry.Helper.readValueFromDisk(
        regEntry.asInstanceOf[DiskEntry], phdrRow)
      if (value != Token.TOMBSTONE) {

        def getFromDVD(dvd: DataValueDescriptor, i: Integer): Any = {
          val field = sch(i)
          logInfo(s"1891: at index ${i} datatype is ${field.dataType} ")
          field.dataType match {
            case ShortType =>
              dvd.getShort
            case ByteType =>
              dvd.getByte
            case _ =>
              dvd.getObject
          }
        }

        if (value.isInstanceOf[Array[Array[Byte]]]) {
          val valueArr = value.asInstanceOf[Array[Array[Byte]]]
          rowFormatter.getColumns(valueArr, dvdArr, (1 to sch.size).toArray)
          val row = Row.fromSeq(dvdArr.zipWithIndex.map { case (dvd, i) => getFromDVD(dvd, i) })
          logInfo(s"1891: rowasseq[][]: ${row}")
          result += row
        } else {
          val valueArr = value.asInstanceOf[Array[Byte]]
          rowFormatter.getColumns(valueArr, dvdArr, (1 to sch.size).toArray)
          // dvd gets gemfire data types
          val row = Row.fromSeq(dvdArr.zipWithIndex.map { case (dvd, i) => getFromDVD(dvd, i) })
          logInfo(s"1891: rowasseq[]: ${row}")
          result += row
        }
      }
    }
  }

  /**
   * Reads data from col buffer regions and appends result to provided ArrayBuffer
   *
   * @param phdrCol PlaceHolderDiskRegion of column batch
   * @param result  ArrayBuffer to which the rows read from phdrCol are appended
   */
  def readColData(phdrCol: PlaceHolderDiskRegion, result: mutable.ArrayBuffer[Row]): Unit = {
    import scala.collection.JavaConversions._
    var batchCounts = mutable.Map[ColumnFormatKey, Integer]()
    val regMap = phdrCol.getRegionMap
    var kkk = regMap.keySet
    val keys = kkk.map(k => k.asInstanceOf[ColumnFormatKey])
    val statsKeys = keys.filter(_.getColumnIndex == -1)
    var tbl: Array[Array[Any]] = null

    var totalRowCount = 0
    for (statsKey <- statsKeys) {
      val cfk = statsKey.asInstanceOf[ColumnFormatKey]
      val regEntry = regMap.getEntry(cfk)
      val regValue = DiskEntry.Helper.readValueFromDisk(regEntry.asInstanceOf[DiskEntry], phdrCol)
          .asInstanceOf[ColumnFormatValue]
      val numStatsColumns = sch.size * ColumnStatsSchema.NUM_STATS_PER_COLUMN + 1
      val stats = org.apache.spark.sql.collection.SharedUtils
          .toUnsafeRow(regValue.getBuffer, numStatsColumns)
      val numOfRows = stats.getInt(0)
      logInfo(s"1891: regmap key = ${statsKey} and entry in map is ${cfk.getUuid} -> $numOfRows")
      batchCounts += (cfk -> numOfRows)
      totalRowCount += numOfRows
    }

    logInfo(s"1891: totalRowCount = ${totalRowCount}")
    tbl = Array.ofDim[Any](totalRowCount, sch.length)

    for ((statsKey, batchCount) <- batchCounts) {
      val cfk = statsKey.asInstanceOf[ColumnFormatKey]

      for ((field, colIndx) <- sch.zipWithIndex) {
        logInfo(s"1891: keys size = ${keys.size}")
        val columnKey = keys.filter(k => k.getColumnIndex == colIndx + 1 &&
            k.getUuid == cfk.getUuid &&
            k.getPartitionId == cfk.getPartitionId
        ).head
        val entry = regMap.getEntry(columnKey)
        var valueBuffer = entry._getValue().asInstanceOf[ColumnFormatValue]
            .getValueRetain(FetchRequest.DECOMPRESS).getBuffer
        // -3 for delete at columnbatch
        val decoder = ColumnEncoding.getColumnDecoder(valueBuffer, field)
        val valueArray = if (valueBuffer == null || valueBuffer.isDirect) {
          logInfo(s"1891: valueBuffers is direct : ${valueBuffer.isDirect}")
          null
        } else {
          valueBuffer.array()
        }
        (0 until batchCount).map(rowNum => {
          logInfo(s"1891: adding prev col batch to tbl rowNum = ${rowNum}")
          // tbl(rowNum)(colNum - 1) = decoder.readInt(valueArray, rowNum)
          tbl(rowNum)(colIndx) = getDecodedValue(
            decoder, valueArray, sch(colIndx).dataType, rowNum
          )
        })
      }
    }
    if (tbl != null && !tbl.equals(Array.ofDim[Any](totalRowCount, sch.length))) {
      logInfo("1891: adding prev col batch to result")
      tbl.foreach(arr => result += Row.fromSeq(arr.toSeq))
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Any] = {
    var result: mutable.ArrayBuffer[Row] = new mutable.ArrayBuffer[Row]()
    logInfo("1891: started compute()", new Throwable)
    // TODO: hmeka think of better way to build iterator
    val diskStrs = Misc.getGemFireCache.listDiskStores()
    var diskStrCol: DiskStoreImpl = null
    var diskStrRow: DiskStoreImpl = null

    logInfo(s"1891: paths  = ${Misc.getRegionPath(tblName)} : ${tblName} : ${dbTableName}")

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

    import scala.collection.JavaConversions._
    for (d <- diskStrs) {
      val dskRegMap = d.getAllDiskRegions
      for ((_, adr) <- dskRegMap) {
        val adrPath = adr.getFullPath
        val adrUnescapePath = PartitionedRegionHelper.unescapePRPath(adrPath)
        if ((adrUnescapePath.equals(colRegPath))
            && adr.isBucket) {
          logInfo(s"1891: matches with col region constructed: ${adrUnescapePath}")
          diskStrCol = d
        } else if (adrUnescapePath.equals(rowRegPath)) {
          logInfo(s"1891: matches with row region constructed: ${adrUnescapePath}")
          diskStrRow = d
        } else if (!adr.isBucket && adrUnescapePath
            .equals('/' + dbTableName.replace('.', '/'))) {
          diskStrRow = d
        } else {
          logInfo(s"1891: region doesn't match with constructed path: ${adrUnescapePath}")
        }
      }
    }
    assert(diskStrRow != null, s"1891: row disk store is null")
    val phdrRow = getPlaceHolderDiskRegion(diskStrRow, rowRegPath)
    logInfo(s"1891: phdrRow is null = ${phdrRow == null} ")

    readRowData(phdrRow, result)
    if (provider.equalsIgnoreCase("COLUMN")) {
      assert(diskStrCol != null, s"1891: col disk store is null")
      val phdrCol = getPlaceHolderDiskRegion(diskStrCol, colRegPath)
      logInfo(s"1891: phdrcol is null = ${phdrCol == null} ")
      readColData(phdrCol, result)
    }
    logInfo(s"1891: split number from compute is ${split.index} and result size is ${result.size}")
    result.iterator
  }

  /**
   * For a given StructField datatype this method reads appropriate
   * value from provided Byte[]
   *
   * @param decoder  decoder for the given Byte[]
   * @param value    Byte[] read from the region
   * @param dataType datatype of the column which is to be decoded
   * @param rowNum   next non null position in Byte[]
   * @return decoded value of the given datatype
   */
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
      case TimestampType =>
        // TODO figure out why decoder gives 1000 x value
        val lv = decoder.readTimestamp(value, rowNum) / 1000
        logInfo(s"1891: long value of timestamp = ${lv}")
        new Timestamp(lv)
      case StringType => decoder.readUTF8String(value, rowNum)
      case DateType =>
        val daysSinceEpoch = decoder.readDate(value, rowNum)
        logInfo(s"for date col, days from epoch = ${daysSinceEpoch}")
        new java.sql.Date(1L * daysSinceEpoch * 24 * 60 * 60 * 1000)
      case d: DecimalType if (d.precision <= Decimal.MAX_LONG_DIGITS) =>
        decoder.readLongDecimal(value, d.precision, d.scale, rowNum)
      case d: DecimalType =>
        decoder.readDecimal(value, d.precision, d.scale, rowNum)
      case _ => null
    }
  }

  /**
   * Returns number of buckets for a given schema and table name
   *
   * @param schemaName schema name which the table belongs to
   * @param tableName  name of the table
   * @return number of buckets
   */
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

  /**
   * Returns number of buckets for a given schema and table name
   *
   * @param schemaName schema name which the table belongs to
   * @param tableName  name of the table
   * @return number of buckets
   */
  override protected def getPartitions: Array[Partition] = {
    val schemaName = dbTableName.split('.')(0)
    val tableName = dbTableName.split('.')(1)
    val numBuckets = RecoveryService.getNumBuckets(schemaName, tableName)
    logInfo(s"1891: numbuckets = ${numBuckets}")
    val parr = (0 until numBuckets).map { p =>
      val prefNodes = null // RecoveryService.getExecutorHost(dbTableName, p)
      logInfo(s"1891: prefNodes = ${prefNodes}")
      val buckets = new mutable.ArrayBuffer[Int](numBuckets)
      buckets += p
      // new MultiBucketExecutorPartition(p, buckets, numBuckets, prefNodes)
      // TODO: need to enforce prefNodes
      new Partition {
        override def index: Int = p
      }
    }.toArray[Partition]
    parr
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    super.write(kryo, output)
    output.writeString(tblName)
    output.writeString(dbTableName)
    output.writeString(provider)
    StructTypeSerializer.write(kryo, output, sch)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)
    tblName = input.readString()
    dbTableName = input.readString()
    provider = input.readString()
    sch = StructTypeSerializer.read(kryo, input, c = null)
    logWarning(s"1891: sch = ${sch}")
  }
}