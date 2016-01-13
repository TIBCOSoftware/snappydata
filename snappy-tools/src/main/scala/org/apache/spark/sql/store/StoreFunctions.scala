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

/**
 * Created by rishim on 3/11/15.
 */
object StoreFunctions {

  implicit def executeWithConnection[T: ClassTag](getConnection: () => Connection, f: PartialFunction[(Connection), T], closeOnSuccess: Boolean = true): T = {
    val conn = getConnection()
    var isClosed = false;
    try {
      f(conn)
    } catch {
      case t: Throwable => {
        conn.close()
        isClosed = true
        throw t;
      }
    } finally {
      if (closeOnSuccess && !isClosed) {
        conn.close()
      }
    }
  }

}
