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
package io.snappydata

import com.pivotal.gemfirexd.FabricService
import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServiceImpl
import io.snappydata.impl.ServerImpl

/**
 * Basic management of store servers which is common to
 * all modes of operation (embedded, local, split cluster etc)
 */
object ServerManager {

  private[snappydata] val contextLock = new AnyRef

  /**
   * Get the singleton instance of `Server`.
   */
  def getServerInstance: Server = {
    var instance: FabricService = FabricServiceImpl.getInstance
    if (instance != null) {
      return checkServerInstance(instance)
    }
    contextLock.synchronized {
      instance = FabricServiceImpl.getInstance
      if (instance == null) {
        val server: Server = new ServerImpl
        FabricServiceImpl.setInstance(server)
        return server
      }
      return checkServerInstance(instance)
    }
  }

  private def checkServerInstance(instance: FabricService): Server = {
    instance match {
      case server: Server => server
      case _ => throw new IllegalStateException(
        s"Found an instance of another snappy component $instance.")
    }
  }
}
