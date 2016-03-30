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
package org.apache.spark.sql.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils

final class SocketStreamSource extends StreamPlanProvider {
  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String],
      schema: StructType): SocketStreamRelation = {
    new SocketStreamRelation(sqlContext, options, schema)
  }
}

final class SocketStreamRelation(
    @transient override val sqlContext: SQLContext,
    options: Map[String, String],
    override val schema: StructType)
    extends StreamBaseRelation(options) {

  val hostname: String = options.get("hostname").get // .getOrElse("localhost")

  val port: Int = options.get("port").map(_.toInt).get // .getOrElse(9999)
  // TODO: Yogesh, revisit these defaults

  import scala.reflect.runtime.{universe => ru}

  val CONVERTER = "converter"

  override protected def createRowStream(): DStream[InternalRow] =
    if (options.exists(_._1 == CONVERTER)) {
      val converter = Utils.getContextOrSparkClassLoader.loadClass(
        options(CONVERTER)).newInstance().asInstanceOf[StreamConverter]
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
      context.socketStream(hostname, port, converter.convert,
        storageLevel).flatMap(rowConverter.toRows)
    }
    else {
      context.socketTextStream(hostname, port,
        storageLevel).flatMap(rowConverter.toRows)
    }
}
