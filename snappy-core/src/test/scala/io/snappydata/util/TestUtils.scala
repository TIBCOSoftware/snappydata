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
package io.snappydata.util

import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.hive.ExternalTableType

object TestUtils {

  def dropAllTables(snc: => SnappyContext): Unit = {
    val sc = SnappyContext.globalSparkContext
    if (sc != null && !sc.isStopped) {
      // drop all the stream tables that can have dependents at the end
      val streams = snc.catalog.getExternalTables(Seq(ExternalTableType.Stream))
      snc.catalog.getTables(None).filter(t => !streams.exists(_.toString ==
          t._1)).foreach(t => snc.dropTable(t._1, ifExists = true))
      streams.foreach(s => snc.dropTable(s.toString, ifExists = true))
    }
  }
}
