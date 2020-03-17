/*
 * Copyright (c) 2017-2020 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.hive

import java.lang.reflect.Type

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, NullType}

/**
 * Helper methods for hive package access.
 */
object HiveAccessUtil extends HiveInspectors {

  override def javaClassToDataType(clz: Class[_]): DataType = clz match {
    case c: Class[_] if classOf[Row].isAssignableFrom(c) => NullType // indicates StructType
    case _ => super.javaClassToDataType(clz)
  }
}
