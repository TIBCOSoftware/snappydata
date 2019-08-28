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
package org.apache.spark.sql.streaming

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SnappyContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils

final class SocketStreamSource extends StreamPlanProvider with DataSourceRegister {

  override def shortName(): String = SnappyContext.SOCKET_STREAM_SOURCE

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String],
      schema: StructType): SocketStreamRelation = {
    new SocketStreamRelation(sqlContext, options, schema)
  }
}

final class SocketStreamRelation(
    @transient override val sqlContext: SQLContext,
    opts: Map[String, String],
    override val schema: StructType)
    extends StreamBaseRelation(opts) {

  val hostname: String = options("hostname")

  val port: Int = options.get("port").map(_.toInt).get

  val T = options("T")

  override protected def createRowStream(): DStream[InternalRow] = {
    val t: ClassTag[Any] = ClassTag(Utils.getContextOrSparkClassLoader.loadClass(T))
    context.socketStream[Any](hostname, port, getStreamConverter.convert,
      storageLevel)(t).mapPartitions { iter =>
      val encoder = RowEncoder(schema)
      // need to call copy() below since there are builders at higher layers
      // (e.g. normal Seq.map) that store the rows and encoder reuses buffer
      iter.flatMap(rowConverter.toRows(_).iterator.map(
        encoder.toRow(_).copy()))
    }
  }

  private def getStreamConverter : StreamConverter = {
    import scala.reflect.runtime.{universe => ru}
    val converter = Utils.getContextOrSparkClassLoader.loadClass(
      options("converter")).newInstance().asInstanceOf[StreamConverter]
    val clazz: Class[_] = converter.getTargetType
    val mirror = ru.runtimeMirror(clazz.getClassLoader)
    val sym = mirror.staticClass(clazz.getName) // obtain class symbol for `c`
    val tpe = sym.selfType // obtain type object for `c`
    ru.TypeTag[Product](mirror, new reflect.api.TypeCreator {
      def apply[U <: reflect.api.Universe with Singleton](m:
      reflect.api.Mirror[U]) = {
        assert(m eq mirror, s"TypeTag[$tpe] defined in $mirror " +
            s"cannot be migrated to $m.")
        tpe.asInstanceOf[U#Type]
      }
    })
    converter
  }
}
