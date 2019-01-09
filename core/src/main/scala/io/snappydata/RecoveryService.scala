/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package io.snappydata

import java.net.URI

import io.snappydata.sql.catalog.CatalogObjectType

object RecoveryService {

  def collectViewsAndRecoverDDLs(): Unit = {
    // Send a message to all the servers and locators to send back their
    // respective persistent state information.
    // Lead should then resolve the member who should be creating the hive ddls
    // Send a message to that member and expect the ddls
    // Keep all the ddls
    // Transform a copy of the ddls to be used in the recovery mode
    // Let the lead come up
  }

  /* All the ddls which will be put in the recovery mode catalog only
     Should include
     1. table def ( replace snappy 'using' with oplog.
     2. VIEWs
     3. Schemas ??
     4. External tables
     5. UDFs / UDAFs
     6. Deploy Jars

     7. Security related ddls not required as the recovery mode is supposed
        to come up using only admin/su credentials.
  */
  def getHiveDDLs: Array[String] = {
    null
  }

  /* This should dump the real ddls into the o/p file */
  def dumpAllDDLs(path: URI): Unit = {

  }

  /* fqtn and bucket number for PR r Column table, -1 indicates replicated row table */
  def getExecutorHost(tableName: String, bucketId: Int = -1): String = {
    null
  }

  /* Table type, PR or replicated, DStore name, numBuckets */
  def getTableDiskInfo(fqtn: String):
  Tuple4[CatalogObjectType.Type, Boolean, String, Int] = {
    null
  }
}
