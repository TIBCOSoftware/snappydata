/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.types

import java.math.MathContext
import java.util.Properties

import scala.reflect.runtime.universe._

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.pivotal.gemfirexd.internal.engine.store.{AbstractCompactExecRow, RowFormatter}
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ColumnDescriptor
import com.pivotal.gemfirexd.internal.impl.jdbc.Util
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.CodegenSupport
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String


object TypeUtilities {

  private[spark] val (rddIdField, rddStorageLevelField) = {
    val c = classOf[RDD[_]]
    val fields = c.getDeclaredFields
    val f1 = fields.find(f => f.getName == "_id" || f.getName == "id").get
    val f2 = fields.find(_.getName.endsWith("storageLevel")).get
    f1.setAccessible(true)
    f2.setAccessible(true)
    (f1, f2)
  }

  private[spark] val (parentMethod, parentSetter) = {
    val c = classOf[CodegenSupport]
    val m = c.getDeclaredMethod("parent")
    m.setAccessible(true)
    val s = c.getDeclaredMethod("parent_$eq", classOf[CodegenSupport])
    s.setAccessible(true)
    (m, s)
  }

  def getMetadata[T](key: String, metadata: Metadata): Option[T] = {
    metadata.map.get(key).asInstanceOf[Option[T]]
  }

  def putMetadata(key: String, value: Any, metadata: Metadata): Metadata = {
    new Metadata(metadata.map + (key -> value))
  }

  def writeMetadata(metadata: Metadata, kryo: Kryo, output: Output): Unit = {
    val map = metadata.map
    if ((metadata eq Metadata.empty) || map.isEmpty) {
      output.writeVarInt(0, true)
    } else {
      output.writeVarInt(map.size, true)
      metadata.map.foreach { case (key, value) =>
        output.writeString(key)
        kryo.writeClassAndObject(output, value)
      }
    }
  }

  def readMetadata(kryo: Kryo, input: Input): Metadata = {
    var mapLen = input.readVarInt(true)
    if (mapLen <= 0) {
      Metadata.empty
    } else {
      val map = Map.newBuilder[String, Any]
      while (mapLen > 0) {
        val key = input.readString()
        val value = kryo.readClassAndObject(input)
        map += key -> value
        mapLen -= 1
      }
      new Metadata(map.result)
    }
  }

  def writeProperties(props: Properties, output: Output): Unit = {
    if (props != null) {
      val keys = props.stringPropertyNames()
      output.writeVarInt(keys.size(), true)
      val keysIterator = keys.iterator()
      while (keysIterator.hasNext) {
        val key = keysIterator.next()
        output.writeString(key)
        output.writeString(props.getProperty(key))
      }
    } else {
      output.writeVarInt(0, true)
    }
  }

  def readProperties(input: Input): Properties = {
    val props = new Properties()
    var numProperties = input.readVarInt(true)
    while (numProperties > 0) {
      val key = input.readString()
      props.setProperty(key, input.readString())
      numProperties -= 1
    }
    props
  }

  def isFixedWidth(dataType: DataType): Boolean = {
    dataType match {
      case x: AtomicType => typeOf(x.tag) match {
        case t if t =:= typeOf[Boolean] => true
        case t if t =:= typeOf[Byte] => true
        case t if t =:= typeOf[Short] => true
        case t if t =:= typeOf[Int] => true
        case t if t =:= typeOf[Long] => true
        case t if t =:= typeOf[Float] => true
        case t if t =:= typeOf[Double] => true
        case t if t =:= typeOf[Decimal] => true
        case _ => false
      }
      case _ => false
    }
  }

  private def assertCharType(cd: ColumnDescriptor): Unit = {
    cd.columnType.getTypeId.getTypeFormatId match {
      case StoredFormatIds.CHAR_TYPE_ID | StoredFormatIds.LONGVARCHAR_TYPE_ID |
           StoredFormatIds.VARCHAR_TYPE_ID | StoredFormatIds.CLOB_TYPE_ID =>
      case _ => throw Util.generateCsSQLException(SQLState.LANG_FORMAT_EXCEPTION,
        "UTF8String", cd.getColumnName)
    }
  }

  private def readUTF8String(rf: RowFormatter, index: Int, bytes: Array[Byte]): UTF8String = {
    val cd = rf.columns(index)
    val offsetFromMap = rf.positionMap(index)
    val offsetAndWidth = rf.getOffsetAndWidth(index, bytes, offsetFromMap, cd, false)
    if (offsetAndWidth >= 0) {
      val columnWidth = offsetAndWidth.toInt
      val offset = (offsetAndWidth >>> Integer.SIZE).toInt
      assertCharType(cd)
      // TODO: SW: SQLChar should be full UTF8 else below is broken for > 3-character UTF8
      UTF8String.fromAddress(bytes, Platform.BYTE_ARRAY_OFFSET + offset, columnWidth)
    } else {
      if (offsetAndWidth == RowFormatter.OFFSET_AND_WIDTH_IS_NULL) null
      else {
        assert(offsetAndWidth == RowFormatter.OFFSET_AND_WIDTH_IS_DEFAULT)
        val defaultBytes = cd.columnDefaultBytes
        if (defaultBytes ne null) {
          UTF8String.fromAddress(defaultBytes, Platform.BYTE_ARRAY_OFFSET, defaultBytes.length)
        } else null
      }
    }
  }

  private def readUTF8String(rf: RowFormatter, index: Int,
      byteArrays: Array[Array[Byte]]): UTF8String = {
    val cd = rf.columns(index)
    if (!cd.isLob) {
      readUTF8String(rf, index, byteArrays(0))
    } else {
      val offsetFromMap = rf.positionMap(index)
      val bytes =
        if (offsetFromMap != 0) byteArrays(offsetFromMap) else cd.columnDefaultBytes
      if (bytes ne null) {
        assertCharType(cd)
        UTF8String.fromAddress(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length)
      } else null
    }
  }

  def readUTF8String(row: AbstractCompactExecRow, index: Int): UTF8String = {
    val rf = row.getRowFormatter
    row.getBaseByteSource match {
      case bytes: Array[Byte] => readUTF8String(rf, index, bytes)
      case byteArrays: Array[Array[Byte]] => readUTF8String(rf, index, byteArrays)
      case s => throw new UnsupportedOperationException(
        s"readUTF8String(AbstractCompactExecRow): unexpected source: $s")
    }
  }

  val mathContextCache: Array[MathContext] = Array.tabulate[MathContext](
    DecimalType.MAX_PRECISION)(i => new MathContext(i + 1))
}
