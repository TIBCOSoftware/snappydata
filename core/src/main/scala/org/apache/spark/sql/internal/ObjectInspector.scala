/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.internal


import org.apache.spark.sql.{Row, AnalysisException}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._

/**
 * Most of the code is copied from HiveInspectors.scala. However we need to support complex data types like map, list and StructType here.
 * There is a PR in Spark https://github.com/apache/spark/pull/1915 regarding this.
 */
object ObjectInspector {

  def javaClassToDataType(clz: Class[_]): DataType = clz match {
    // java class
    case c: Class[_] if c == classOf[java.lang.String] => StringType
    case c: Class[_] if c == classOf[java.sql.Date] => DateType
    case c: Class[_] if c == classOf[java.sql.Timestamp] => TimestampType
    case c: Class[_] if c == classOf[java.math.BigDecimal] => DecimalType.SYSTEM_DEFAULT
    case c: Class[_] if c == classOf[Array[Byte]] => BinaryType
    case c: Class[_] if c == classOf[java.lang.Short] => ShortType
    case c: Class[_] if c == classOf[java.lang.Integer] => IntegerType
    case c: Class[_] if c == classOf[java.lang.Long] => LongType
    case c: Class[_] if c == classOf[java.lang.Double] => DoubleType
    case c: Class[_] if c == classOf[java.lang.Byte] => ByteType
    case c: Class[_] if c == classOf[java.lang.Float] => FloatType
    case c: Class[_] if c == classOf[java.lang.Boolean] => BooleanType

    // primitive type
    case c: Class[_] if c == java.lang.Short.TYPE => ShortType
    case c: Class[_] if c == java.lang.Integer.TYPE => IntegerType
    case c: Class[_] if c == java.lang.Long.TYPE => LongType
    case c: Class[_] if c == java.lang.Double.TYPE => DoubleType
    case c: Class[_] if c == java.lang.Byte.TYPE => ByteType
    case c: Class[_] if c == java.lang.Float.TYPE => FloatType
    case c: Class[_] if c == java.lang.Boolean.TYPE => BooleanType

    case c: Class[_] if c.isArray => ArrayType(javaClassToDataType(c.getComponentType))

    // Hive seems to return this for struct types?
    case c: Class[_] if c == classOf[java.lang.Object] => NullType

    // java list type unsupported
    case c: Class[_] if c == classOf[java.util.List[_]] =>
      throw new AnalysisException(
        "List type in java is unsupported because " +
            "JVM type erasure makes spark fail to catch a component type in List<>")

    // java map type unsupported
    case c: Class[_] if c == classOf[java.util.Map[_, _]] =>
      throw new AnalysisException(
        "Map type in java is unsupported because " +
            "JVM type erasure makes spark fail to catch key and value types in Map<>")

    case c => throw new AnalysisException(s"Unsupported java type $c")
  }

  def externalDataTypeFor(dt: DataType): DataType = dt match {
    case _ if ScalaReflection.isNativeType(dt) => dt
    case TimestampType => ObjectType(classOf[java.sql.Timestamp])
    case DateType => ObjectType(classOf[java.sql.Date])
    case _: DecimalType => ObjectType(classOf[java.math.BigDecimal])
    case StringType => ObjectType(classOf[java.lang.String])
    case _: ArrayType => ObjectType(classOf[scala.collection.Seq[_]])
    case _: MapType => ObjectType(classOf[scala.collection.Map[_, _]])
    case _: StructType => ObjectType(classOf[Row])
    case p: PythonUserDefinedType => externalDataTypeFor(p.sqlType)
    case udt: UserDefinedType[_] => ObjectType(udt.userClass)
  }

}
