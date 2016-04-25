/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package io.snappydata.impl

import java.util

import com.pivotal.gemfirexd.snappy.ComplexTypeSerializer

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Implementation of <code>ComplexTypeSerializer</code>.
 */
final class ComplexTypeSerializerImpl(dataType: DataType)
    extends ComplexTypeSerializer {

  def this(tp: java.lang.Object) = this(SparkShellRDDHelper.getDataType(tp))

  private[this] val (serializer, converter) = CodeGeneration
      .getComplexTypeSerializer(dataType)

  private[this] val struct = dataType match {
    case s: StructType => Some(s)
    case _ => None
  }

  private[this] val bufferHolder = new BufferHolder()

  private[this] def toBytes(v: Any): Array[Byte] = {
    serializer.serialize(converter(v), bufferHolder)
    val b = util.Arrays.copyOf(bufferHolder.buffer, bufferHolder.totalSize())
    bufferHolder.reset()
    b
  }

  override def getBytes[T](
      v: Array[T with java.lang.Object]): Array[Byte] = struct match {
    case Some(s) => toBytes(new GenericRowWithSchema(
      v.asInstanceOf[Array[Any]], s))
    case _ => toBytes(v)
  }

  override def getBytes[T](v: util.Collection[T]): Array[Byte] = struct match {
    case Some(s) => toBytes(new GenericRowWithSchema(
      v.toArray.asInstanceOf[Array[Any]], s))
    case _ => toBytes(v)
  }

  override def getBytes[K, V](v: util.Map[K, V]): Array[Byte] = toBytes(v)

  override def getBytes(v: Any): Array[Byte] = struct match {
    case Some(s) => v match {
      case a: Array[Any] => toBytes(new GenericRowWithSchema(a, s))
      case a: Array[_] => toBytes(new GenericRowWithSchema(a.toSeq.toArray, s))
      case l: Seq[_] => toBytes(new GenericRowWithSchema(l.toArray, s))
      case c: util.Collection[_] => toBytes(new GenericRowWithSchema(
        c.toArray.asInstanceOf[Array[Any]], s))
      case _ => toBytes(v)
    }
    case _ => toBytes(v)
  }
}
