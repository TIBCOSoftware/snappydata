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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks._

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.internal.cache._
import com.pivotal.gemfirexd.internal.catalog.UUID
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.store._
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.{ColumnDescriptor, ColumnDescriptorList}
import com.pivotal.gemfirexd.internal.iapi.types.{DataType => _, _}
import scala.annotation.meta.param
import scala.io.Source

import com.gemstone.gemfire.internal.cache.DiskEntry.Helper
import com.gemstone.gemfire.internal.cache.store.SerializedDiskBuffer
import com.gemstone.gemfire.internal.offheap.ByteSource
import com.gemstone.gemfire.internal.shared.FetchRequest
import com.pivotal.gemfirexd.internal.client.am.Types
import io.snappydata.recovery.RecoveryService
import io.snappydata.recovery.RecoveryService.mostRecentMemberObject
import io.snappydata.thrift.CatalogTableObject

import org.apache.spark.serializer.StructTypeSerializer
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.{SerializedArray, SerializedMap, SerializedRow}
import org.apache.spark.sql.execution.RDDKryo
import org.apache.spark.sql.execution.columnar.encoding.{ColumnDecoder, ColumnDeleteDecoder, ColumnDeltaDecoder, ColumnEncoding, ColumnStatsSchema, UpdatedColumnDecoder}
import org.apache.spark.sql.execution.columnar.impl.{ColumnDelta, ColumnFormatEntry, ColumnFormatKey, ColumnFormatValue}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SnappySession}
import org.apache.spark.unsafe.Platform
import org.apache.spark.{Partition, TaskContext}

class OpLogRdd(
    @transient private val session: SnappySession,
    private var fqtn: String,
    private var internalFQTN: String,
    private var schema: StructType,
    private var provider: String,
    private var projection: Array[Int],
    @transient private[sql] val filters: Array[Expression],
    private[sql] var fullScan: Boolean,
    @(transient@param) partitionPruner: () => Int,
    var tableSchemas: mutable.Map[String, StructType],
    var versionMap: mutable.Map[String, Int],
    var tableColIdsMap: mutable.Map[String, Array[Int]])
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
      case IntegerType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER, isNullable)
      case BooleanType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN, isNullable)
      case ByteType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.SMALLINT, isNullable)
      case FloatType => DataTypeDescriptor.getSQLDataTypeDescriptor("float", isNullable)
      case BinaryType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BLOB, isNullable)
      case DoubleType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE, isNullable)
      case ShortType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.SMALLINT, isNullable)
      case TimestampType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP,
        isNullable)
      case DateType => DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DATE, isNullable)
      case d: DecimalType =>
        val precision = d.precision
        val scale = d.scale
        new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.DECIMAL),
          precision, scale, isNullable, precision)
      case StringType =>
        if (metadata.contains("base")) {
          metadata.getString("base") match {
            case "STRING" | "CLOB" =>
              DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CLOB, isNullable)
            case "VARCHAR" =>
              DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, isNullable,
                metadata.getLong("size").toInt)
          }
        }
        else {
          // when - create table using column as select ..- is used,
          // it create string column with no base information in metadata
          DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CLOB, isNullable)
        }
      case _: ArrayType | _: MapType | _: StructType =>
        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BLOB, isNullable)
      case _ => new DataTypeDescriptor(TypeId.CHAR_ID, isNullable)
    }
  }

  def getProjectColumnId(tableName: String, columnName: String): Int = {
    val fqtnLowerKey = tableName.replace(".", "_").toLowerCase
    assert(versionMap.contains(fqtnLowerKey))
    val maxVersion = versionMap.getOrElse(fqtnLowerKey, null)
    assert(maxVersion != null)
    var index = -1
    val fieldsArr = tableSchemas.getOrElse(s"$maxVersion#$fqtnLowerKey", null).fields
    breakable {
      for (i <- fieldsArr.indices) {
        if (fieldsArr(i).name.toUpperCase() == columnName.toUpperCase()) {
          index = i
          break()
        } else {
          index = -1
        }
      }
    }
    // todo : handle properly
    tableColIdsMap.getOrElse(s"$maxVersion#$fqtnLowerKey", null)(index)
  }

  def getSchemaColumnId(tableName: String, colName: String, version: Int): Int = {
    val fqtnLowerKey = tableName.replace('.', '_').toLowerCase
    var index = -1
    val fieldsArr = tableSchemas.getOrElse(s"$version#$fqtnLowerKey", null).fields
    breakable {
      for (i <- fieldsArr.indices) {
        if (fieldsArr(i).name.toUpperCase() == colName.toUpperCase()) {
          index = i
          break()
        } else {
          index = -1
        }
      }
    }
    if (index != -1) tableColIdsMap.getOrElse(s"$version#$fqtnLowerKey", null)(index)
    else -1
  }

  /**
   * Method creates and returns a RowFormatter from schema
   *
   * @return RowFormatter
   */
  def getRowFormatter(versionNum: Int, schemaStruct: StructType): RowFormatter = {
    val cdl = new ColumnDescriptorList()
    schemaStruct.toList.foreach(field => {
      val cd = new ColumnDescriptor(
        field.name,
        schemaStruct.fieldIndex(field.name) + 1,
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
    if (provider.equalsIgnoreCase("ROW")) {
      cdl.add(null, new ColumnDescriptor("SNAPPYDATA_INTERNAL_ROWID", schemaStruct.size + 1,
        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT, false),
        null,
        null,
        null.asInstanceOf[UUID],
        null.asInstanceOf[UUID],
        0L,
        0L,
        0L,
        false))
    }
    val schemaName = fqtn.split('.')(0)
    val tableName = fqtn.split('.')(1)
    new RowFormatter(cdl, schemaName, tableName, versionNum, null, false)
  }

  def getFromDVD(schema: StructType, dvd: DataValueDescriptor, i: Integer,
      valueArr: Array[Array[Byte]], complexSchema: Seq[StructField]): Any = {
    val field = schema(i)
    val complexFieldIndex = if (complexSchema == null) 0 else complexSchema.indexOf(field) + 1
    field.dataType match {
      case ShortType => if (dvd.isNull) null else dvd.getShort
      case ByteType => if (dvd.isNull) null else dvd.getByte
      case a: ArrayType =>
        assert(field.dataType == a)
        val array = valueArr(complexFieldIndex)
        val data = new SerializedArray(8)
        if (array != null) {
          data.pointTo(array, Platform.BYTE_ARRAY_OFFSET, array.length)
          data.toArray(a.elementType)
        } else null
      case m: MapType =>
        assert(field.dataType == m)
        val map = valueArr(complexFieldIndex)
        val data = new SerializedMap()
        if (map != null) {
          data.pointTo(map, Platform.BYTE_ARRAY_OFFSET)
          val jmap = new java.util.HashMap[Any, Any](data.numElements())
          data.foreach(m.keyType, m.valueType, (k, v) => jmap.put(k, v))
          jmap
        } else null
      case s: StructType =>
        assert(field.dataType == s)
        val struct = valueArr(complexFieldIndex)
        if (struct != null) {
          val data = new SerializedRow(4, s.length)
          data.pointTo(struct, Platform.BYTE_ARRAY_OFFSET, struct.length)
          data
        } else null
      case BinaryType =>
        val blobValue = dvd.getObject.asInstanceOf[HarmonySerialBlob]
        Source.fromInputStream(blobValue.getBinaryStream).map(e => e.toByte).toArray
      case _ => dvd.getObject
    }
  }

  def getRow(valueArr: Any): Row = {
    val valueIsGrid = valueArr.isInstanceOf[Array[Array[Byte]]]
    val versionNum: Int = if (valueIsGrid) {
      RowFormatter.readCompactInt(valueArr.asInstanceOf[Array[Array[Byte]]](0), 0)
    } else {
      RowFormatter.readCompactInt(valueArr.asInstanceOf[Array[Byte]], 0)
    }
    assert(versionNum >= 0 || versionNum == RowFormatter.TOKEN_RECOVERY_VERSION,
      "unexpected schemaVersion=" + versionNum + " for RowFormatter#readVersion")

    val tableKey = versionNum + "#" + fqtn.toLowerCase.replace(".", "_")
    assert(tableSchemas.contains(tableKey),
      s"schema of $fqtn for Rowformatter version=$versionNum unavailable")
    var schemaOfVersion = tableSchemas(tableKey)
    // todo: build a local cache = table ->rowformatters
    // todo: so we don't have to create rowformatters for every record

    // For row tables external catalog stores:
    // float gets stored as DoubleType
    // byte gets stored as ShortType
    // tinyint gets store as ShortType
    if ("row".equalsIgnoreCase(provider)) {
      val correctedFields = schemaOfVersion.map(field => {
        field.dataType match {
          case FloatType =>
            if (field.metadata.contains("originalSqlType")) {
              if (field.metadata.getString("originalSqlType").equals("real")) field
              else StructField(field.name, DoubleType, field.nullable, field.metadata)
            } else field
          case ByteType => StructField(field.name, ShortType, field.nullable, field.metadata)
          case _ => field
        }
      }).toArray
      schemaOfVersion = StructType(correctedFields)
    }
    val rowFormatter = getRowFormatter(versionNum, schemaOfVersion)
    val dvdArr = new Array[DataValueDescriptor](schemaOfVersion.length)
    val projectColumns: Array[String] = schema.fields.map(_.name)
    val complexSch = schemaOfVersion.filter(f =>
      f.dataType match {
        case _: ArrayType | _: MapType | _: StructType | _: StringType => true
        case _ => false
      }
    )

    if (valueIsGrid) {
      rowFormatter.getColumns(
        valueArr.asInstanceOf[Array[Array[Byte]]], dvdArr, (1 to schemaOfVersion.size).toArray)
      val row = Row.fromSeq(dvdArr.zipWithIndex.map { case (dvd, i) =>
        getFromDVD(schemaOfVersion, dvd, i, valueArr.asInstanceOf[Array[Array[Byte]]], complexSch)
      })
      formatFinalRow(row, projectColumns, versionNum, schemaOfVersion)
    } else {
      rowFormatter.getColumns(
        valueArr.asInstanceOf[Array[Byte]], dvdArr, (1 to schemaOfVersion.size).toArray)
      val row = Row.fromSeq(dvdArr.zipWithIndex.map { case (dvd, i) =>
        getFromDVD(schemaOfVersion, dvd, i, null, null)
      })
      formatFinalRow(row, projectColumns, versionNum, schemaOfVersion)
    }
  }

  /**
   * Reads data from row buffer regions and appends result to provided ArrayBuffer
   *
   * @param phdrRow PlaceHolderDiskRegion of row
   */
  def iterateRowData(phdrRow: PlaceHolderDiskRegion): Iterator[Row] = {
    val rm = phdrRow.getRegionMap
    val regionMapEntries = rm.regionEntries()
    assert(rm != null, "regionMap for row placeHolderDiskRegion is null")
    assert(regionMapEntries != null, "regionMap entries for row placeHolderDiskRegion is null")
    val regMapItr = regionMapEntries.iterator().asScala
    regMapItr.map { regEntry =>
      DiskEntry.Helper.readValueFromDisk(
        regEntry.asInstanceOf[DiskEntry], phdrRow) match {
        case valueArr@(_: Array[Byte] | _: Array[Array[Byte]]) => getRow(valueArr)
        case Token.TOMBSTONE => null
      }
    }.filter(_ ne null)
  }

  def formatFinalRow(row: Row, projectColumns: Array[String],
      versionNum: Int, schStruct: StructType): Row = {
    val resArr = new Array[Any](projectColumns.length)
    var i = 0
    projectColumns.foreach(projectCol => {
      val projectColId = getProjectColumnId(fqtn.toLowerCase(), projectCol)
      val schemaColId = getSchemaColumnId(fqtn.toLowerCase(), projectCol, versionNum)
      val colValue = if (projectColId == schemaColId) {
        // col is from latest schema not previous/dropped column
        // todo remove case conversion
        val colNamesArr = schStruct.fields.map(_.name.toLowerCase)
        row(colNamesArr.indexOf(projectCol))
      } else null
      resArr(i) = colValue
      i += 1
    })
    Row.fromSeq(resArr.toSeq)
  }

  def getValueInVMOrDiskWithoutFaultIn(phdr: PlaceHolderDiskRegion, entry: RegionEntry): Any = {
    val regEntry = phdr.getDiskEntry(entry.getKey)
    val rawValue = false
    val faultin = false
    var v = DiskEntry.Helper.getValueRetain(regEntry, phdr, rawValue)
    val isRemovedFromDisk = Token.isRemovedFromDisk(v)
    if ((v == null || isRemovedFromDisk) /* && !phdrCol.isIndexCreationThread() */ ) {
      regEntry.synchronized {
        v = DiskEntry.Helper.getValueRetain(regEntry, phdr, rawValue)
        if (v == null) {
          v = Helper.getOffHeapValueOnDiskOrBuffer(
            regEntry, phdr.getDiskRegionView, phdr, faultin, rawValue)
        }
      }
    }
    if (isRemovedFromDisk) v = null else if (v.isInstanceOf[ByteSource]) {
      val bs = v.asInstanceOf[ByteSource]
      val deserVal = bs.getDeserializedForReading
      if (deserVal ne v) {
        bs.release()
        v = deserVal
      }
    }
    return v
  }

  /**
   * Reads data from col buffer regions and appends result to provided ArrayBuffer
   *
   * @param phdrCol PlaceHolderDiskRegion of column batch
   */
  def iterateColData(phdrCol: PlaceHolderDiskRegion): Iterator[Row] = {
    val regMap = phdrCol.getRegionMap
    assert(regMap != null, "region map for column batch is null")
    if (regMap.regionEntries().isEmpty) return Iterator.empty
    regMap.keySet().iterator().asScala.flatMap {
      case k: ColumnFormatKey if k.getColumnIndex == ColumnFormatEntry.STATROW_COL_INDEX =>
        // get required info about deletes
        val delKey = k.withColumnIndex(ColumnFormatEntry.DELETE_MASK_COL_INDEX)
        val delEntry = regMap.getEntry(delKey)
        val (deleteBuffer, deleteDecoder) = if (delEntry ne null) {
          val regValue = DiskEntry.Helper.readValueFromDisk(delEntry.asInstanceOf[DiskEntry],
            phdrCol).asInstanceOf[ColumnFormatValue]
          val valueBuffer = regValue.asInstanceOf[ColumnFormatValue]
              .getValueRetain(FetchRequest.DECOMPRESS).getBuffer
          valueBuffer -> new ColumnDeleteDecoder(valueBuffer)
        } else (null, null)
        // get required info about columns
        var columnIndex = 1
        var hasTombstone = false
        val decodersAndValues = schema.map { field =>
          val columnKey = k.withColumnIndex(columnIndex)
          columnIndex += 1
          val entry = regMap.getEntry(columnKey)
          if (!hasTombstone && entry.isTombstone) {
            hasTombstone = true
          }
          if (hasTombstone) null
          else {
            val value = getValueInVMOrDiskWithoutFaultIn(phdrCol, entry)
                .asInstanceOf[ColumnFormatValue]
            val valueBuffer = value.getValueRetain(FetchRequest.DECOMPRESS).getBuffer
            val decoder = ColumnEncoding.getColumnDecoder(valueBuffer, field)
            val valueArray = if (valueBuffer == null || valueBuffer.isDirect) {
              null
            } else {
              valueBuffer.array()
            }
            decoder -> valueArray
          }
        }

        if (hasTombstone) Iterator.empty
        else {
          val statsEntry = regMap.getEntry(k)
          val statsValue = DiskEntry.Helper.readValueFromDisk(statsEntry.asInstanceOf[DiskEntry],
            phdrCol).asInstanceOf[ColumnFormatValue]
          val numStatsColumns = schema.size * ColumnStatsSchema.NUM_STATS_PER_COLUMN + 1
          val stats = org.apache.spark.sql.collection.SharedUtils
              .toUnsafeRow(statsValue.getBuffer, numStatsColumns)
          val numOfRows = stats.getInt(0)
          val deletedCount = if ((deleteDecoder ne null) && (deleteBuffer ne null)) {
            val allocator = ColumnEncoding.getAllocator(deleteBuffer)
            ColumnEncoding.readInt(allocator.baseObject(deleteBuffer),
              allocator.baseOffset(deleteBuffer) + deleteBuffer.position() + 8)
          } else 0
          var currentDeleted = 0
          val colNullCounts = Array.fill[Int](schema.size)(0)

          val updatedDecoders = schema.indices.map { colIndx =>
            val deltaColIndex = ColumnDelta.deltaColumnIndex(colIndx, 0)
            val deltaEntry1 = regMap.getEntry(k.withColumnIndex(deltaColIndex))
            val delta1 = if (deltaEntry1 ne null) {
              DiskEntry.Helper.readValueFromDisk(deltaEntry1.asInstanceOf[DiskEntry],
                phdrCol).asInstanceOf[ColumnFormatValue].getBuffer
            } else null

            val deltaEntry2 = regMap.getEntry(k.withColumnIndex(deltaColIndex - 1))
            val delta2 = if (deltaEntry2 ne null) {
              DiskEntry.Helper.readValueFromDisk(deltaEntry2.asInstanceOf[DiskEntry],
                phdrCol).asInstanceOf[ColumnFormatValue].getBuffer
            } else null

            val updateDecoder = if ((delta1 ne null) || (delta2 ne null)) {
              UpdatedColumnDecoder(decodersAndValues(colIndx)._1, schema(colIndx), delta1, delta2)
            } else null
            updateDecoder
          }

          var adjustedRowIndex = 0
          (0 until (numOfRows - deletedCount)).map { i =>
            while ((deleteDecoder ne null) && deleteDecoder.deleted(i + currentDeleted)) {
              // null counts should be added as we go even for deleted records
              // because it is required to build indexes in colbatch
              Row.fromSeq(schema.indices.map { colIndx =>
                val decoderAndValue = decodersAndValues(colIndx)
                val colDecoder = decoderAndValue._1
                val colNextNullPosition = colDecoder.getNextNullPosition
                if (i + currentDeleted == colNextNullPosition) {
                  colNullCounts(colIndx) += 1
                  colDecoder.findNextNullPosition(
                    decoderAndValue._2, colNextNullPosition, colNullCounts(colIndx))
                }
              })
              // calculate how many consecutive rows to skip so that
              // i+numDeletd points to next un deleted row
              currentDeleted += 1
              adjustedRowIndex = i + currentDeleted
            }
            Row.fromSeq(schema.indices.map { colIndx =>
              val decoderAndValue = decodersAndValues(colIndx)
              val colDecoder = decoderAndValue._1
              val colArray = decoderAndValue._2
              val colNextNullPosition = colDecoder.getNextNullPosition
              val fieldIsNull = i + currentDeleted == colNextNullPosition
              if (fieldIsNull) {
                colNullCounts(colIndx) += 1
                colDecoder.findNextNullPosition(
                  colArray, colNextNullPosition, colNullCounts(colIndx))
              }

              val updatedDecoder = updatedDecoders(colIndx)
              if ((updatedDecoder ne null) && !updatedDecoder.unchanged(i + currentDeleted) &&
                  updatedDecoder.readNotNull) {
                val uv = getUpdatedValue(updatedDecoder.getCurrentDeltaBuffer, schema(colIndx))
                uv
              } else {
                val decodedValue = if (fieldIsNull) null else getDecodedValue(colDecoder,
                  colArray, schema(colIndx).dataType, i + currentDeleted - colNullCounts(colIndx))
                decodedValue
              }
            })
          }.toIterator
        }
      case _ => Iterator.empty
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    try {
      val diskStores = Misc.getGemFireCache.listDiskStores()
      var diskStrCol: DiskStoreImpl = null
      var diskStrRow: DiskStoreImpl = null
      val tableName = fqtn.split('.')(1)

      val colRegPath = if (Misc.getRegionPath(internalFQTN.toUpperCase) == null) {
        throw new IllegalStateException(s"regionPath for $internalFQTN not found")
      } else {
        val regionPath = Misc.getRegionPath(internalFQTN.toUpperCase)
        s"/_PR//B_${regionPath.substring(1, regionPath.length - 1)}/_${split.index}"
      }
      val rowRegPath = s"/_PR//B_${fqtn.replace('.', '/')}/${split.index}"

      var phdrRow: PlaceHolderDiskRegion = null
      var phdrCol: PlaceHolderDiskRegion = null

      for (d <- diskStores.asScala) {
        val dskRegMap = d.getAllDiskRegions
        for ((_, adr) <- dskRegMap.asScala) {
          val adrPath = adr.getFullPath
          var adrUnescapePath = PartitionedRegionHelper.unescapePRPath(adrPath)
          // var adrUnescapePath = adrPath
          // unescapePRPath replaces _ in db or table name with /
          val wrongTablePattern = tableName.replace('_', '/')
          if (tableName.contains('_') && adrUnescapePath.contains(wrongTablePattern)) {
            adrUnescapePath = adrUnescapePath
                .replace(wrongTablePattern, tableName.replace('/', '_'))
          }
          if (adrUnescapePath.equals(colRegPath) && adr.isBucket) {
            diskStrCol = d
            phdrCol = adr.asInstanceOf[PlaceHolderDiskRegion]
          } else if (adrUnescapePath.equals(rowRegPath)) {
            diskStrRow = d
            phdrRow = adr.asInstanceOf[PlaceHolderDiskRegion]
          } else if (!adr.isBucket && adrUnescapePath
              .equals('/' + fqtn.replace('.', '/'))) {
            diskStrRow = d
            phdrRow = adr.asInstanceOf[PlaceHolderDiskRegion]
          }
        }
      }
      assert(diskStrRow != null, s"Row disk store is null. row region path not found:$rowRegPath")
      assert(phdrRow != null, s"PlaceHolderDiskRegion not found for regionPath=$rowRegPath")
      val rowIter: Iterator[Row] = iterateRowData(phdrRow)

      if ("column".equalsIgnoreCase(provider)) {
        assert(diskStrCol != null, s"col disk store is null")
        val colIter = iterateColData(phdrCol)
        rowIter ++ colIter
      } else {
        rowIter
      }
    } catch {
      // in case of error log and return empty iterator. cluster shouldn't go down
      case x@(_: Exception | _: AssertionError) =>
        logError(s"Unable to read $fqtn.", x)
        Seq.empty.iterator
    }
  }

  def getUpdatedValue(currentDeltaBuffer: ColumnDeltaDecoder, field: StructField): Any = {
    field.dataType match {
      case LongType => currentDeltaBuffer.readLong
      case IntegerType => currentDeltaBuffer.readInt
      case BooleanType => currentDeltaBuffer.readBoolean
      case ByteType => currentDeltaBuffer.readByte
      case FloatType => currentDeltaBuffer.readFloat
      case DoubleType => currentDeltaBuffer.readDouble
      case BinaryType => currentDeltaBuffer.readBinary
      case ShortType => currentDeltaBuffer.readShort
      case TimestampType => new Timestamp(currentDeltaBuffer.readTimestamp / 1000)
      case StringType => currentDeltaBuffer.readUTF8String
      case DateType => val daysSinceEpoch = currentDeltaBuffer.readDate
        new java.sql.Date(1L * daysSinceEpoch * 24 * 60 * 60 * 1000)
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        currentDeltaBuffer.readLongDecimal(d.precision, d.scale)
      case d: DecimalType => currentDeltaBuffer.readDecimal(d.precision, d.scale)
      case _ => null
    }
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
      rowNum: Int): Any = {
    if (decoder.isNullAt(value, rowNum)) null
    else dataType match {
      case LongType => decoder.readLong(value, rowNum)
      case IntegerType => decoder.readInt(value, rowNum)
      case BooleanType => decoder.readBoolean(value, rowNum)
      case ByteType => decoder.readByte(value, rowNum)
      case FloatType => decoder.readFloat(value, rowNum)
      case DoubleType => decoder.readDouble(value, rowNum)
      case BinaryType => decoder.readBinary(value, rowNum)
      case ShortType => decoder.readShort(value, rowNum)
      case TimestampType =>
        val lv = decoder.readTimestamp(value, rowNum) / 1000
        new Timestamp(lv)
      case StringType => decoder.readUTF8String(value, rowNum)
      case DateType =>
        val daysSinceEpoch = decoder.readDate(value, rowNum)
        new java.sql.Date(1L * daysSinceEpoch * 24 * 60 * 60 * 1000)
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        decoder.readLongDecimal(value, d.precision, d.scale, rowNum)
      case d: DecimalType => decoder.readDecimal(value, d.precision, d.scale, rowNum)
      case a: ArrayType => decoder.readArray(value, rowNum).toArray(a.elementType)
      case _: MapType => decoder.readMap(value, rowNum)
      case s: StructType => decoder.readStruct(value, s.length, rowNum)
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

  def getPartitionEvaluator: () => Array[Partition] = () => getPartitions

  /**
   * Returns number of buckets for a given schema and table name
   *
   * @return number of buckets
   */
  override protected def getPartitions: Array[Partition] = {
    val schemaName = fqtn.split('.')(0)
    val tableName = fqtn.split('.')(1)
    val (numBuckets, _) = RecoveryService.getNumBuckets(schemaName, tableName)
    val partition = (0 until numBuckets).map { p =>
      new Partition {
        override def index: Int = p
      }
    }.toArray[Partition]
    partition
  }

  /**
   * Returns seq of hostnames where the corresponding
   * split/bucket is present.
   *
   * @param split partition corresponding to bucket
   * @return sequence of hostnames
   */
  override def getPreferredLocations(split: Partition): Seq[String] =
    RecoveryService.getExecutorHost(fqtn, split.index)

  override def write(kryo: Kryo, output: Output): Unit = {
    super.write(kryo, output)
    output.writeString(internalFQTN)
    output.writeString(fqtn)
    output.writeString(provider)
    output.writeInt(tableSchemas.size)
    tableSchemas.iterator.foreach(ele => {
      output.writeString(ele._1)
      StructTypeSerializer.write(kryo, output, ele._2)
    })
    output.writeInt(versionMap.size)
    versionMap.iterator.foreach(ele => {
      output.writeString(ele._1)
      output.writeInt(ele._2)
    })
    output.writeInt(tableColIdsMap.size)
    tableColIdsMap.foreach(ele => {
      output.writeString(ele._1)
      output.writeInt(ele._2.length)
      output.writeInts(ele._2)
    })
    StructTypeSerializer.write(kryo, output, schema)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)
    internalFQTN = input.readString()
    fqtn = input.readString()
    provider = input.readString()
    val schemaMapSize = input.readInt()
    tableSchemas = collection.mutable.Map().empty
    for (_ <- 0 until schemaMapSize) {
      val k = input.readString()
      val v = StructTypeSerializer.read(kryo, input, c = null)
      tableSchemas.put(k, v)
    }
    val versionMapSize = input.readInt()
    versionMap = collection.mutable.Map.empty
    for (_ <- 0 until versionMapSize) {
      val k = input.readString()
      val v = input.readInt()
      versionMap.put(k, v)
    }
    val tableColIdsMapSize = input.readInt()
    tableColIdsMap = collection.mutable.Map.empty
    for (_ <- 0 until tableColIdsMapSize) {
      val k = input.readString()
      val vlength = input.readInt()
      val v = input.readInts(vlength)
      tableColIdsMap.put(k, v)
    }
    schema = StructTypeSerializer.read(kryo, input, c = null)
  }
}