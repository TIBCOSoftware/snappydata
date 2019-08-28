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

import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServiceImpl
import com.pivotal.gemfirexd.{FabricLocator, FabricServer}

import org.apache.spark.sql.execution.columnar.impl.StoreCallback

// TODO: Documentation
trait Server extends FabricServer with StoreCallback

// TODO: Documentation
trait Lead extends Server

// TODO: Documentation
trait Locator extends FabricLocator

trait ProtocolOverrides extends FabricServiceImpl {

  abstract override def getProtocol: String = Constant.DEFAULT_EMBEDDED_URL

  abstract override def getNetProtocol: String = Constant.DEFAULT_THIN_CLIENT_URL
}
