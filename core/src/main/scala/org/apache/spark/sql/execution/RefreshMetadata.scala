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
package org.apache.spark.sql.execution

import com.gemstone.gemfire.cache.execute.FunctionContext
import com.gemstone.gemfire.distributed.DistributedMember
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ddl.catalog.GfxdSystemProcedures
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdMessage
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.sql.execute.FunctionUtils
import com.pivotal.gemfirexd.internal.engine.sql.execute.FunctionUtils.GetFunctionMembers

import org.apache.spark.SparkContext
import org.apache.spark.sql.collection.{ToolsCallbackInit, Utils}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.hive.SnappyHiveExternalCatalog
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.{LocalMode, SnappyContext, SnappyEmbeddedMode, SnappySession}

@SerialVersionUID(-46264515900419559L)
object RefreshMetadata extends Enumeration
    with com.gemstone.gemfire.cache.execute.Function with GetFunctionMembers {

  val ID: String = "SnappyRefreshMetadata"

  type Type = Value
  // list of various action types possible with this function
  // first argument to the execute() methods should be this
  val UPDATE_CATALOG_SCHEMA_VERSION, FLUSH_ROW_BUFFER, REMOVE_CACHED_OBJECTS, CLEAR_CODEGEN_CACHE,
  ADD_URIS_TO_CLASSLOADER, REMOVE_FUNCTION_JAR, REMOVE_URIS_FROM_CLASSLOADER = Value

  override def getId: String = ID

  override def execute(context: FunctionContext): Unit = {
    val args = context.getArguments.asInstanceOf[(Type, Any)]
    executeLocal(args._1, args._2)
    context.getResultSender[Boolean].lastResult(true)
  }

  /**
   * EMBEDDED MODE ONLY: Execute on all members of the embedded cluster excluding locators.
   */
  def executeOnAllEmbedded(action: Type, args: Any): Unit = {
    try {
      FunctionUtils.onMembers(Misc.getDistributedSystem, this, false)
          .withArgs(action -> args).execute(ID).getResult
    } catch {
      case e: Exception =>
        // do local execution in any case
        executeLocal(action, args)
        throw e
    }
  }

  /**
   * Execute on all members of the cluster excluding locators. An optional argument
   * specifies whether execution has to be done in connector mode or not.
   */
  def executeOnAll(sc: SparkContext, action: Type, args: Any,
      executeInConnector: Boolean = true): Unit = {
    if (sc eq null) executeOnAllEmbedded(action, args)
    else SnappyContext.getClusterMode(sc) match {
      case SnappyEmbeddedMode(_, _) => executeOnAllEmbedded(action, args)
      case LocalMode(_, _) => executeLocal(action, args)
      case _ => if (executeInConnector) {
        executeLocal(action, args)
        Utils.mapExecutors[Unit](sc, () => {
          executeLocal(action, args)
          Iterator.empty
        })
      }
    }
  }

  def executeLocal(action: Type, args: Any): Unit = {
    action match {
      case UPDATE_CATALOG_SCHEMA_VERSION =>
        SnappySession.clearAllCache(onlyQueryPlanCache = true)
        CodeGeneration.clearAllCache()
        if (args != null) {
          val (version, relations) = args.asInstanceOf[(Long, Seq[(String, String)])]
          // update the version stored in own profile
          val profile = GemFireXDUtils.getMyProfile(false)
          if (profile ne null) {
            profile.updateCatalogSchemaVersion(version)
          }
          val catalog = SnappyHiveExternalCatalog.getInstance
          if (catalog ne null) {
            if (relations.isEmpty) catalog.invalidateAll()
            else relations.foreach(catalog.invalidate)
          }
        }
      case FLUSH_ROW_BUFFER =>
        GfxdSystemProcedures.flushLocalBuckets(args.asInstanceOf[String], true)
      case REMOVE_CACHED_OBJECTS =>
        ExternalStoreUtils.removeCachedObjects(args.asInstanceOf[String])
      case CLEAR_CODEGEN_CACHE =>
        CodeGeneration.clearAllCache()
      case ADD_URIS_TO_CLASSLOADER =>
        ToolsCallbackInit.toolsCallback.addURIsToExecutorClassLoader(
          args.asInstanceOf[Array[String]])
      case REMOVE_FUNCTION_JAR =>
        ToolsCallbackInit.toolsCallback.removeFunctionJars(
          args.asInstanceOf[Array[String]])
      case REMOVE_URIS_FROM_CLASSLOADER =>
        ToolsCallbackInit.toolsCallback.removeURIsFromExecutorClassLoader(
          args.asInstanceOf[Array[String]])
    }
  }

  override def hasResult: Boolean = true

  override def optimizeForWrite(): Boolean = false

  override def isHA: Boolean = true

  override def getMembers: java.util.Set[DistributedMember] = GfxdMessage.getAllGfxdServers

  override def getServerGroups: java.util.Set[String] = null

  override def postExecutionCallback(): Unit = {}
}
