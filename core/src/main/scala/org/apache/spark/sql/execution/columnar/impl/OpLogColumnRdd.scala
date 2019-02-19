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
package org.apache.spark.sql.execution.columnar.impl

import java.nio.ByteBuffer
import java.util
import java.util.Collections

import com.esotericsoftware.kryo.KryoSerializable
import com.gemstone.gemfire.internal.cache._
import com.gemstone.gemfire.internal.concurrent.CustomEntryConcurrentHashMap
import com.gemstone.gemfire.internal.shared.FetchRequest
import com.pivotal.gemfirexd.internal.catalog.UUID
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.store._
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.{ColumnDescriptor, ColumnDescriptorList}
import com.pivotal.gemfirexd.internal.iapi.types.{DataTypeDescriptor, DataValueDescriptor, TypeId}
import io.snappydata.recovery.RecoveryService
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.{SnappyContext, SnappySession}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.collection.{MultiBucketExecutorPartition, Utils}
import org.apache.spark.sql.execution.RDDKryo
import org.apache.spark.sql.execution.columnar.ColumnBatchIterator
import org.apache.spark.sql.execution.columnar.encoding.{ColumnDeleteDecoder, ColumnEncoding}
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.store.StoreUtils.getBucketPreferredLocations
import org.apache.spark.sql.types._

import scala.annotation.meta.param
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class OpLogColumnRdd(
                      @transient private val session: SnappySession,
                      private var fQTN: String,
                      private var schema: StructType,
                      private var projection: Array[Int],
                      @transient private[sql] val filters: Array[Expression],
                      private[sql] var fullScan: Boolean,
                      @(transient@param) partitionPruner: () => Int)
  extends RDDKryo[Any](session.sparkContext, Nil) with KryoSerializable {

  var rowFormatter: RowFormatter = null

  def getDVDType(dataType: DataType): DataTypeDescriptor = {
    dataType match {
      case IntegerType => DataTypeDescriptor.INTEGER
      case _ => new DataTypeDescriptor(TypeId.VARCHAR_ID, true)
    }
  }


  def getRowFormatter: RowFormatter = {
    val cdl = new ColumnDescriptorList()
    schema.toList.foreach(field => {
      val cd = new ColumnDescriptor(
        field.name,
        schema.fieldIndex(field.name),
        getDVDType(field.dataType),
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
    println(s"columndescriptor list = ${cdl}")
    val schemaName = fQTN.split("\\.")(0)
    val tableName = fQTN.split("\\.")(1)
    new RowFormatter(cdl, schemaName, tableName, 0, null, false)

  }

  private def fillRowUsingByteArrayArray(
                                          row: AbstractCompactExecRow,
                                          value: Array[Array[Byte]]): Unit = {
    if (rowFormatter.hasLobs) {
      println("has lobs")
      row.setRowArray(value, rowFormatter)
    } else {
      println("no lobs")
      row.setRowArray(value(0), rowFormatter)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Any] = {
    if (rowFormatter == null) {
      rowFormatter = getRowFormatter
    }

    val colReg = Misc.getRegionForTable(fQTN, true).asInstanceOf[PartitionedRegion]
    val rowBuffReg = colReg.getColocatedWithRegion

    val rowBucketReg = rowBuffReg.getDataStore.getLocalBucketById(0)
    val rowRegEntriesItr = rowBucketReg.getRegionMap.regionEntries().iterator()

    // TODO: hmeka think of better way to build iterator
    var l: Seq[Seq[Any]] = Seq.empty

    def getFromDataValueDescriptor(dvd: DataValueDescriptor) = {
      dvd.getClass match {
        case int => dvd.getInt
        case _ => println(s"1891: add handling"); dvd.getString
      }
    }

    // row buffer data
    while (rowRegEntriesItr.hasNext) {
      val regEntry = rowRegEntriesItr.next()
      val value = DiskEntry.Helper.readValueFromDisk(
        regEntry.asInstanceOf[DiskEntry], rowBucketReg)
      if (value != Token.TOMBSTONE) {
        val dvdArr = new Array[DataValueDescriptor](schema.length)
        if (value.isInstanceOf[Array[Array[Byte]]]) {
          val valueArr = value.asInstanceOf[Array[Array[Byte]]]
          val row = new com.pivotal.gemfirexd.internal.engine.store.CompactExecRow()
          row.setRowFormatter(rowFormatter)
          fillRowUsingByteArrayArray(row, valueArr)
        } else {
          println("from row buffer : []")
          val valueArr = value.asInstanceOf[Array[Byte]]
          rowFormatter.getColumns(valueArr, dvdArr, Seq(1, 2).toArray)
          // dvd gets gemfire data types
          val rowAsSeq = dvdArr.map(dvd => getFromDataValueDescriptor(dvd)).toSeq
          l = (l :+ rowAsSeq)
        }
      }
    }


    // get bucket id from partition
    val colBucketReg = colReg.getDataStore.getLocalBucketById(0)
    val colRegEntriesItr = colBucketReg.getRegionMap.regionEntries().iterator()
    var numOfRows = -1
    var tbl: Array[Array[Any]] = null

    while (colRegEntriesItr.hasNext) {
      val regEntry = colRegEntriesItr.next()
      val value = DiskEntry.Helper.readValueFromDisk(regEntry.asInstanceOf[DiskEntry], colBucketReg)
      val numStatsColumns = 7

      val scanColBuf = org.apache.spark.sql.collection.SharedUtils
        .toUnsafeRow(value.asInstanceOf[ColumnFormatValue].getBuffer, numStatsColumns)
      val colNum = regEntry.getKey.asInstanceOf[ColumnFormatKey].getColumnIndex
      if (colNum == -1 || colNum < 0) { // stats buffer
        numOfRows = scanColBuf.getInt(0)
        if (tbl != null && !tbl.equals(Array.ofDim[Any](numOfRows, schema.length))) {
          tbl.foreach(arr => l = l :+ arr.toSeq)
        }
        tbl = Array.ofDim[Any](numOfRows, schema.length)
      } else if (colNum >= 0) {
        var buff1 = value.asInstanceOf[ColumnFormatValue]
          .getValueRetain(FetchRequest.DECOMPRESS).getBuffer
        val decoder = ColumnEncoding.getColumnDecoder(buff1, schema(0))
        // -3 for delete at columnbatch
        val buff = if (buff1 == null || buff1.isDirect) {
          null
        } else {
          buff1.array()
        }
        // new ColumnDeleteDecoder(buff)
        (0 until numOfRows).map(rowNum => {
          tbl(rowNum)(colNum - 1) = decoder.readInt(buff, rowNum)
        })
      }
    }
    if (tbl != null && !tbl.equals(Array.ofDim[Any](numOfRows, schema.length))) {
      tbl.foreach(arr => l = l :+ arr.toSeq)
    }
    l.iterator
  Seq((11, 111), (22, 222)).iterator
  }

  // private[this] var allPartitions: Array[Partition] = _
  def getPartitionEvaluator: () => Array[Partition] = () => getPartitions

  override protected def getPartitions: Array[Partition] = {
    logInfo("1891: 100 getpartitions")
    val (_, _, _, numBuckets) = (1, 1, 1, 1) // RecoveryService.getTableDiskInfo(tableName)
    val parr = (0 until numBuckets).map { p =>
      val prefNodes = Seq("localhost") // RecoveryService.getExecutorHost(tableName, p)
    val buckets = new mutable.ArrayBuffer[Int](1)
      buckets += p
      new MultiBucketExecutorPartition(p, buckets, numBuckets, prefNodes)
    }.toArray[Partition]
    logInfo(s"1891: parr is null ${parr == null}")
    parr.zipWithIndex.foreach { case (partition, index) =>
      logInfo(s"1891::: partition:${partition.index} index:${index}")
    }
    parr
  }
}