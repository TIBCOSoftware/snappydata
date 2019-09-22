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
package org.apache.spark.sql.hive

import java.io.File

import scala.collection.JavaConverters._

import io.snappydata.Property
import org.apache.hadoop.hive.ql.exec.FunctionRegistry

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.hive.test.{TestHiveContext, TestHiveSparkSession}
import org.apache.spark.sql.internal.{SharedState, SnappySharedState}
import org.apache.spark.sql.{SnappyContext, SnappySession}

class TestHiveSnappySession(@transient protected val sc: SparkContext,
    protected val loadTestTables: Boolean)
    extends SnappySession(sc) with TestHiveSparkSession {

  assume(enableHiveSupport)

  override protected def existingSharedState: Option[SharedState] = None

  /**
   * State shared across sessions, including the [[SparkContext]], cached data, listener,
   * and a catalog that interacts with external systems.
   */
  @transient
  override lazy val sharedState: SnappySharedState = SnappyContext.sharedState(sparkContext)

  override def hiveDefaultTableFilePath(name: TableIdentifier): String =
    sessionState.hiveState.catalog.hiveDefaultTableFilePath(name)

  override def getCachedDataSourceTable(table: TableIdentifier): LogicalPlan =
    sessionState.hiveState.catalog.getCachedDataSourceTable(table)

  override def metadataHive: HiveClient = sessionState.hiveState.metadataHive

  override def newSession(): SnappySession = new TestHiveSnappySession(sc, loadTestTables)

  override private[sql] def overrideConfs: Map[String, String] =
    TestHiveContext.overrideConfs + (Property.HiveCompatibility.name -> "spark")

  override def reset(): Unit = {
    try {
      // HACK: Hive is too noisy by default.
      org.apache.log4j.LogManager.getCurrentLoggers.asScala.foreach { log =>
        val logger = log.asInstanceOf[org.apache.log4j.Logger]
        if (!logger.getName.contains("org.apache.spark")) {
          logger.setLevel(org.apache.log4j.Level.WARN)
        }
      }

      sharedState.cacheManager.clearCache()
      loadedTables.clear()
      sessionCatalog.clearTempTables()
      sessionCatalog.snappyExternalCatalog.invalidateAll()

      FunctionRegistry.getFunctionNames.asScala.filterNot(originalUDFs.contains(_)).
          foreach { udfName => FunctionRegistry.unregisterTemporaryUDF(udfName) }

      // Some tests corrupt this value on purpose, which breaks the RESET call below.
      sessionState.conf.setConfString("fs.default.name", new File(".").toURI.toString)

      sessionCatalog.setCurrentDatabase("default")
    } catch {
      case e: Exception =>
        logError("FATAL ERROR: Failed to reset TestDB state.", e)
    }
  }
}
