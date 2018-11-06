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
package org.apache.spark.sql.hive

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.{FunctionResourceLoader, GlobalTempViewManager}
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.SQLConf

/**
 * Catalog used when SnappyData Connector mode is used over thin client JDBC connection.
 */
class SnappyConnectorCatalog(externalCatalog: SnappyExternalCatalog,
    snappySession: SnappySession,
    metadataHive: HiveClient,
    globalTempViewManager: GlobalTempViewManager,
    functionResourceLoader: FunctionResourceLoader,
    functionRegistry: FunctionRegistry,
    sqlConf: SQLConf,
    hadoopConf: Configuration)
    extends SnappyStoreHiveCatalog(
      externalCatalog: SnappyExternalCatalog,
      snappySession: SnappySession,
      metadataHive: HiveClient,
      globalTempViewManager: GlobalTempViewManager,
      functionResourceLoader: FunctionResourceLoader,
      functionRegistry: FunctionRegistry,
      sqlConf: SQLConf,
      hadoopConf: Configuration) with ConnectorCatalog
