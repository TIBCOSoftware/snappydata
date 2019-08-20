/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package io.snappydata.test.dunit.standalone;

import java.io.File;
import java.rmi.RemoteException;
import java.util.Properties;

import io.snappydata.test.dunit.BounceResult;
import io.snappydata.test.dunit.DUnitEnv;

public class StandAloneDUnitEnv extends DUnitEnv {

  private DUnitLauncher.MasterRemote master;

  public StandAloneDUnitEnv(DUnitLauncher.MasterRemote master) {
    this.master = master;
  }

  @Override
  public String getLocatorString() {
    return DUnitLauncher.getLocatorString();
  }

  @Override
  public String getLocatorAddress() {
    return "localhost";
  }
  
  @Override
  public int getLocatorPort() {
    return DUnitLauncher.locatorPort;
  }

  @Override
  public Properties getDistributedSystemProperties() {
    return DUnitLauncher.getDistributedSystemProperties();
  }

  @Override
  public int getPid() {
    return Integer.getInteger(DUnitLauncher.VM_NUM_PARAM, -1).intValue();
  }

  @Override
  public int getVMID() {
    return getPid();
  }

  @Override
  public BounceResult bounce(int pid) throws RemoteException {
    return master.bounce(pid);
  }

  @Override
  public File getWorkingDirectory(int pid) {
    return DUnitLauncher.processManager.getVMDir(pid, false);
  }

}
