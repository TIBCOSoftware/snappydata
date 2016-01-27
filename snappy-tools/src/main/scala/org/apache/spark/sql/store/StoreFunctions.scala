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
package org.apache.spark.sql.store

import java.sql.Connection

import scala.reflect.ClassTag
import scala.util.control.NonFatal

object StoreFunctions {

  implicit def executeWithConnection[T: ClassTag](connector: () => Connection,
      f: Connection => T, closeOnSuccess: Boolean = true): T = {
    val conn = connector()
    var isClosed = false
    try {
      f(conn)
    } catch {
      case NonFatal(e) =>
        conn.close()
        isClosed = true
        throw e
    } finally {
      if (closeOnSuccess && !isClosed) {
        conn.close()
      }
    }
  }
}
