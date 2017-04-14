/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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
package io.snappydata.tests;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import io.snappydata.test.dunit.AsyncInvocation;
import io.snappydata.test.dunit.DistributedTestBase;
import io.snappydata.test.dunit.Host;
import io.snappydata.test.dunit.RMIException;
import io.snappydata.test.dunit.SerializableRunnable;
import io.snappydata.test.dunit.VM;
import io.snappydata.test.dunit.standalone.DUnitLauncher;
import io.snappydata.test.util.TestException;

/**
 * This class tests the basic functionality of the distributed unit
 * test framework.
 */
public class BasicDUnitTest extends DistributedTestBase {

  public BasicDUnitTest(String name) {
    super(name);
  }

  public static void startLocator() {
    final int locatorPort = DUnitLauncher.getLocator();
    final String locatorLogFile = "locator-" + locatorPort + ".log";
    DistributedTestBase.invokeInLocator(new SerializableRunnable() {
      @Override
      public void run() {
        Properties p = DUnitLauncher.getDistributedSystemProperties();
        // I never want this locator to end up starting a jmx manager
        // since it is part of the unit test framework
        p.setProperty("jmx-manager", "false");
        try {
          Locator.startLocatorAndDS(locatorPort, new File(locatorLogFile), p);
        } catch (IOException ioe) {
          throw new TestException(ioe.getMessage(), ioe);
        }
      }
    });
  }

  public static void stopLocator() {
    DistributedTestBase.invokeInLocator(new SerializableRunnable() {
      @Override
      public void run() {
        if (Locator.hasLocator()) {
          Locator.getLocator().stop();
        }
      }
    });
  }

  ////////  Test Methods

  /**
   * Tests how the Hydra framework handles an error
   */
  public void _testDontCatchRemoteException() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    vm.invoke(this.getClass(), "remoteThrowException");
  }

  public void testRemoteInvocationWithException() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    try {
      vm.invoke(this.getClass(), "remoteThrowException");
      fail("Should have thrown a BasicTestException");

    } catch (RMIException ex) {
      assertTrue(ex.getCause() instanceof BasicTestException);
    }
  }

  static class BasicTestException extends RuntimeException {
    BasicTestException() {
      this("Test exception.  Please ignore.");
    }

    BasicTestException(String s) {
      super(s);
    }
  }

  /**
   * Accessed via reflection.  DO NOT REMOVE
   */
  protected static void remoteThrowException() {
    String s = "Test exception.  Please ignore.";
    throw new BasicTestException(s);
  }

  // disabled redundant test
  public void DISABLED_testRemoteInvokeAsync() throws InterruptedException {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    String name = this.getUniqueName();
    String value = "Hello";

    startLocator();
    try {
      AsyncInvocation ai =
          vm.invokeAsync(this.getClass(), "remoteBind",
              new Object[]{name, value});
      ai.join();
      if (ai.exceptionOccurred()) {
        fail("remoteBind failed", ai.getException());
      }

      ai = vm.invokeAsync(this.getClass(), "remoteValidateBind",
          new Object[]{name, value});
      ai.join();
      if (ai.exceptionOccurred()) {
        fail("remoteValidateBind failed", ai.getException());
      }

      vm.invoke(this.getClass(), "stopDS", null);
    } finally {
      stopLocator();
    }

  }

  private static InternalDistributedSystem _instancce = null;
  private static Properties bindings = new Properties();

  private static void remoteBind(String name, String s) {
    _instancce = new BasicDUnitTest("bogus").getSystem(); // forces connection
    bindings.setProperty(name, s);
  }

  static void stopDS() {
    if (_instancce != null) {
      _instancce.disconnect();
    }
  }

  private static void remoteValidateBind(String name, String expected) {
    assertEquals(expected, bindings.getProperty(name));
  }

  public void testRemoteInvokeAsyncWithException()
      throws InterruptedException {

    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
//    String name = this.getUniqueName();
//    String value = "Hello";

    AsyncInvocation ai =
        vm.invokeAsync(this.getClass(), "remoteThrowException");
    ai.join();
    assertTrue(ai.exceptionOccurred());
    Throwable ex = ai.getException();
    assertTrue(ex instanceof BasicTestException);
  }
}
