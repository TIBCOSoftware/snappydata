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
 * Portions Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

import java.rmi.Naming;

import com.gemstone.gemfire.internal.shared.NativeCalls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dsmith
 */
public class ChildVM {

  private final static Logger logger = LoggerFactory.getLogger(ChildVM.class);

  public static void main(String[] args) throws Throwable {
    try {
      int namingPort = Integer.getInteger(DUnitLauncher.RMI_PORT_PARAM).intValue();
      int vmNum = Integer.getInteger(DUnitLauncher.VM_NUM_PARAM).intValue();
      int pid = NativeCalls.getInstance().getProcessId();
      logger.info("VM" + vmNum + " is launching" + (pid > 0 ? " with PID " + pid : ""));
      DUnitLauncher.MasterRemote holder = (DUnitLauncher.MasterRemote) Naming.lookup(
          "//localhost:" + namingPort + "/" + DUnitLauncher.MASTER_PARAM);
      DUnitLauncher.init(holder);
      DUnitLauncher.locatorPort = holder.getLocatorPort();
      Naming.rebind("//localhost:" + namingPort + "/vm" + vmNum, new RemoteDUnitVM());
      holder.signalVMReady();
      //This loop is here so this VM will die even if the master is mean killed.
      while (true) {
        holder.ping();
        Thread.sleep(1000);
      }
    } catch (Throwable t) {
      t.printStackTrace();
      System.exit(1);
    }
  }
}
