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
package org.apache.spark.sql.types

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.unsafe.types.UTF8String

/**
 * An internal type to represent VARCHAR() and CHAR() types in
 * column definitions of "CREATE TABLE".
 */
case class CharStringType(override val defaultSize: Int,
    baseType: String) extends AtomicType {

  override private[sql] type InternalType = UTF8String

  @transient override private[sql] lazy val tag = typeTag[InternalType]

  override private[sql] val ordering = implicitly[Ordering[InternalType]]

  override def typeName: String = baseType match {
    case "VARCHAR" => s"varchar($defaultSize)"
    case "CHAR" => s"char($defaultSize)"
    case _ => "string"
  }

  override def sql: String = typeName

  override private[spark] def asNullable: CharStringType = this
}
