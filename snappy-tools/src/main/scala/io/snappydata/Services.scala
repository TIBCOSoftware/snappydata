/*
 * Copyright (c) 2010-2016 SnappyData, Inc. All rights reserved.
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

import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServiceImpl
import com.pivotal.gemfirexd.{FabricLocator, FabricServer}
import io.snappydata.gemxd.ClusterCallback
import org.apache.spark.sql.columntable.StoreCallback

// TODO: Documentation
trait Server extends FabricServer with ClusterCallback with StoreCallback {

}

// TODO: Documentation
trait Lead extends Server {

  def waitUntilPrimary()
}

// TODO: Documentation
trait Locator extends FabricLocator with ClusterCallback {

}

/**
 * Created by soubhikc on 16/10/15.
 */
trait ProtocolOverrides extends FabricServiceImpl {

  abstract override def getProtocol: java.lang.String = {
    "jdbc:snappydata:"
  }

  abstract override def getNetProtocol: String = {
    "jdbc:snappydata:"
  }

  abstract override def getDRDAProtocol: String = {
    return "jdbc:snappydata:drda://"
  }

  abstract override def getThriftProtocol: String = {
    return "jdbc:snappydata:thrift://"
  }
}
