/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package dunit.tests;

import java.util.Properties;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import dunit.AsyncInvocation;
import dunit.DistributedTestBase;
import dunit.Host;
import dunit.RMIException;
import dunit.VM;

/**
 * This class tests the basic functionality of the distributed unit
 * test framework.
 */
public class BasicDUnitTest extends DistributedTestBase {

  public BasicDUnitTest(String name) {
    super(name);
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

  public void _testRemoteInvocationBoolean() {

  }

  public void testRemoteInvokeAsync() throws InterruptedException {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    String name = this.getUniqueName();
    String value = "Hello";

    AsyncInvocation ai =
        vm.invokeAsync(this.getClass(), "remoteBind",
            new Object[]{name, value});
    ai.join();
    // TODO shouldn't we call fail() here?
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
