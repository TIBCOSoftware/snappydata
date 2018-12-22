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
package org.apache.spark.serializer

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.sql.Types

import com.esotericsoftware.kryo.io.{Input, KryoObjectInput, KryoObjectOutput, Output}
import com.esotericsoftware.kryo.{Kryo, KryoException, Serializer => KryoClassSerializer}

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.row.{SnappyStoreClientDialect, SnappyStoreDialect}
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.types._
import org.apache.spark.sql.{PartitionResult, SnappyDataPoolDialect}


private[spark] class ExternalizableOnlySerializer[T <: Externalizable]
    extends KryoClassSerializer[T] {

  private var objectInput: KryoObjectInput = _
  private var objectOutput: KryoObjectOutput = _

  override def write(kryo: Kryo, output: Output, obj: T): Unit = {
    try {
      obj.writeExternal(getObjectOutput(kryo, output))
    } catch {
      case e@(_: ClassCastException | _: IOException) =>
        throw new KryoException(e)
    }
  }

  override def read(kryo: Kryo, input: Input, c: Class[T]): T = {
    try {
      val obj = kryo.newInstance(c)
      obj.readExternal(getObjectInput(kryo, input))
      obj
    } catch {
      case e@(_: ClassCastException | _: ClassNotFoundException |
              _: IOException) => throw new KryoException(e)
    }
  }

  private def getObjectOutput(kryo: Kryo, output: Output): ObjectOutput = {
    if (objectOutput == null) {
      objectOutput = new KryoObjectOutput(kryo, output)
    } else {
      objectOutput.setOutput(output)
    }
    objectOutput
  }

  private def getObjectInput(kryo: Kryo, input: Input): ObjectInput = {
    if (objectInput == null) {
      objectInput = new KryoObjectInput(kryo, input)
    } else {
      objectInput.setInput(input)
    }
    objectInput
  }
}

private[spark] final class ExternalizableResolverSerializer[T <: Externalizable](
    readResolve: T => T) extends ExternalizableOnlySerializer[T] {

  override def read(kryo: Kryo, input: Input, c: Class[T]): T = {
    readResolve(super.read(kryo, input, c))
  }
}

object StructTypeSerializer extends KryoClassSerializer[StructType] {

  def writeType(kryo: Kryo, output: Output, dataType: DataType): Unit = {
    dataType match {
      case IntegerType => output.writeVarInt(Types.INTEGER, false)
      case LongType => output.writeVarInt(Types.BIGINT, false)
      case StringType => output.writeVarInt(Types.CLOB, false)
      case DoubleType => output.writeVarInt(Types.DOUBLE, false)
      case FloatType => output.writeVarInt(Types.FLOAT, false)
      case ShortType => output.writeVarInt(Types.SMALLINT, false)
      case ByteType => output.writeVarInt(Types.TINYINT, false)
      case BooleanType => output.writeVarInt(Types.BOOLEAN, false)
      case BinaryType => output.writeVarInt(Types.BLOB, false)
      case TimestampType => output.writeVarInt(Types.TIMESTAMP, false)
      case DateType => output.writeVarInt(Types.DATE, false)
      case t: DecimalType =>
        output.writeVarInt(Types.DECIMAL, false)
        output.writeVarInt(t.precision, true)
        output.writeVarInt(t.scale, true)
      case a: ArrayType =>
        output.writeVarInt(Types.ARRAY, false)
        writeType(kryo, output, a.elementType)
        output.writeBoolean(a.containsNull)
      case m: MapType =>
        // indicates MapType since there is no equivalent in JDBC
        output.writeVarInt(Types.JAVA_OBJECT, false)
        writeType(kryo, output, m.keyType)
        writeType(kryo, output, m.valueType)
        output.writeBoolean(m.valueContainsNull)
      case s: StructType =>
        output.writeVarInt(Types.STRUCT, false)
        write(kryo, output, s)
      case _ =>
        output.writeVarInt(Types.OTHER, false)
        kryo.writeClassAndObject(output, dataType)
    }
  }

  def readType(kryo: Kryo, input: Input): DataType = {
    input.readVarInt(false) match {
      case Types.INTEGER => IntegerType
      case Types.BIGINT => LongType
      case Types.CLOB => StringType
      case Types.DOUBLE => DoubleType
      case Types.FLOAT => FloatType
      case Types.SMALLINT => ShortType
      case Types.TINYINT => ByteType
      case Types.BOOLEAN => BooleanType
      case Types.BLOB => BinaryType
      case Types.TIMESTAMP => TimestampType
      case Types.DATE => DateType
      case Types.DECIMAL =>
        val precision = input.readVarInt(true)
        val scale = input.readVarInt(true)
        DecimalType(precision, scale)
      case Types.ARRAY =>
        val elementType = readType(kryo, input)
        ArrayType(elementType, input.readBoolean())
      case Types.JAVA_OBJECT => // indicates MapType
        val keyType = readType(kryo, input)
        val valueType = readType(kryo, input)
        MapType(keyType, valueType, input.readBoolean())
      case Types.STRUCT => read(kryo, input, classOf[StructType])
      case Types.OTHER => kryo.readClassAndObject(input).asInstanceOf[DataType]
      case t => throw new KryoException(
        s"Serialization error: unexpected DataType ID $t")
    }
  }

  override def write(kryo: Kryo, output: Output, struct: StructType): Unit = {
    val fields = struct.fields
    val numFields = fields.length
    output.writeVarInt(numFields, true)
    var i = 0
    while (i < numFields) {
      val field = fields(i)
      output.writeString(field.name)
      writeType(kryo, output, field.dataType)
      output.writeBoolean(field.nullable)
      TypeUtilities.writeMetadata(field.metadata, kryo, output)
      i += 1
    }
  }

  override def read(kryo: Kryo, input: Input,
      c: Class[StructType]): StructType = {
    val numFields = input.readVarInt(true)
    val fields = new Array[StructField](numFields)
    var i = 0
    while (i < numFields) {
      val name = input.readString()
      val dataType = readType(kryo, input)
      val nullable = input.readBoolean()
      val metadata = TypeUtilities.readMetadata(kryo, input)
      fields(i) = StructField(name, dataType, nullable, metadata)
      i += 1
    }
    StructType(fields)
  }
}

object PartitionResultSerializer extends KryoClassSerializer[PartitionResult] {

  override def write(kryo: Kryo, output: Output, obj: PartitionResult): Unit = {
    val data = obj._1
    val len = data.length
    output.writeInt(len)
    output.writeBytes(data, 0, len)
    output.writeVarInt(obj._2, true)
  }

  override def read(kryo: Kryo, input: Input,
      c: Class[PartitionResult]): PartitionResult = {
    val len = input.readInt()
    val data = input.readBytes(len)
    new PartitionResult(data, input.readVarInt(true))
  }
}

object ConnectionPropertiesSerializer
    extends KryoClassSerializer[ConnectionProperties] {

  override def write(kryo: Kryo, output: Output,
      connProps: ConnectionProperties): Unit = {
    output.writeString(connProps.url)
    output.writeString(connProps.driver)
    connProps.dialect match {
      case SnappyStoreDialect => output.writeByte(0)
      case SnappyStoreClientDialect => output.writeByte(1)
      case SnappyDataPoolDialect => output.writeByte(3)
      case d => output.writeByte(2)
        kryo.writeClassAndObject(output, d)
    }
    val poolProps = connProps.poolProps
    if (poolProps ne null) {
      val numProps = poolProps.size
      output.writeVarInt(numProps, true)
      if (numProps > 0) {
        for ((key, value) <- poolProps) {
          output.writeString(key)
          output.writeString(value)
        }
      }
    } else {
      output.writeVarInt(0, true)
    }
    // write only executor properties if available since on target side
    // that is the one which will be used
    if (connProps.executorConnProps.isEmpty) {
      TypeUtilities.writeProperties(connProps.connProps, output)
    } else {
      TypeUtilities.writeProperties(connProps.executorConnProps, output)
    }
    output.writeBoolean(connProps.hikariCP)
  }

  override def read(kryo: Kryo, input: Input,
      c: Class[ConnectionProperties]): ConnectionProperties = {
    read(kryo, input)
  }

  def read(kryo: Kryo, input: Input): ConnectionProperties = {
    val url = input.readString()
    val driver = input.readString()
    val dialect = input.readByte() match {
      case 0 => SnappyStoreDialect
      case 1 => SnappyStoreClientDialect
      case 3 => SnappyDataPoolDialect
      case _ => kryo.readClassAndObject(input).asInstanceOf[JdbcDialect]
    }
    var numProps = input.readVarInt(true)
    var poolProps: Map[String, String] = Map.empty
    if (numProps > 0) {
      val propsBuilder = Map.newBuilder[String, String]
      while (numProps > 0) {
        val key = input.readString()
        propsBuilder += key -> input.readString()
        numProps -= 1
      }
      poolProps = propsBuilder.result
    }
    val connProps = TypeUtilities.readProperties(input)
    val hikariCP = input.readBoolean()
    ConnectionProperties(url, driver, dialect, poolProps, connProps,
      connProps, hikariCP)
  }
}
