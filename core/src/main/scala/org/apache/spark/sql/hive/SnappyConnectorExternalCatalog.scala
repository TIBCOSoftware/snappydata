/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.catalog.CatalogFunction
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.{SnappyContext, SnappySession}

private[spark] class SnappyConnectorExternalCatalog(var cl: HiveClient,
    hadoopConf: Configuration) extends SnappyExternalCatalog(cl, hadoopConf) {

  override def createFunction(
      db: String,
      funcDefinition: CatalogFunction): Unit = {
    val functionName = funcDefinition.identifier.funcName
    val className = funcDefinition.className
    // contains only one URI
    val jarURI = funcDefinition.resources.head.uri
    val sessionCatalog = SnappyContext(null: SparkContext).snappySession
        .sessionCatalog.asInstanceOf[ConnectorCatalog]
    sessionCatalog.connectorHelper.executeCreateUDFStatement(db, functionName, className, jarURI)
    SnappySession.clearAllCache()
  }

  override def dropFunction(db: String, name: String): Unit = {
    val sessionCatalog = SnappyContext(null: SparkContext).snappySession
        .sessionCatalog.asInstanceOf[ConnectorCatalog]
    sessionCatalog.connectorHelper.executeDropUDFStatement(db, name)
    SnappySession.clearAllCache()
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = {}
}
