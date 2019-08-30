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
package io.snappydata.impl

import java.sql.SQLException
import java.util.Properties

import com.gemstone.gemfire.cache.execute.FunctionService
import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServerImpl
import io.snappydata.util.ServiceUtils
import io.snappydata.{ProtocolOverrides, Server}

import org.apache.spark.sql.execution.RefreshMetadata
import org.apache.spark.sql.row.SnappyStoreDialect

/**
 * This class ties up few things that is Snappy specific.
 * for e.g. Connection url & ClusterCallback
 */
class ServerImpl extends FabricServerImpl with Server with ProtocolOverrides {

  @throws(classOf[SQLException])
  override def start(bootProperties: Properties): Unit = {
    SnappyStoreDialect.init()
    start(bootProperties, ignoreIfStarted = false)
  }

  @throws(classOf[SQLException])
  override def start(bootProps: Properties, ignoreIfStarted: Boolean): Unit = {
    // all SnappyData distributed GemFire Functions should be registered below
    FunctionService.registerFunction(RefreshMetadata)
    super.start(ServiceUtils.setCommonBootDefaults(bootProps, forLocator = false),
      ignoreIfStarted)
  }

  override def isServer: Boolean = true
}
