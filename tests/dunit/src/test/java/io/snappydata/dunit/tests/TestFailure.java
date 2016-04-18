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
package io.snappydata.dunit.tests;

import io.snappydata.test.dunit.DistributedTestBase;

/**
 * The tests in this class always fail.  It is used when developing
 * DUnit to give us an idea of how test failure are logged, etc.
 *
 * @author David Whitlock
 *
 * @since 3.0
 */
public class TestFailure extends DistributedTestBase {

  public TestFailure(String name) {
    super(name);
  }

  ////////  Test Methods

  public void testFailure() {
    assertTrue("Test Failure", false);
  }

  public void testError() {
    String s = "Test Error";
    throw new Error(s);
  }

  public void testHang() throws InterruptedException {
    Thread.sleep(100000 * 1000);
  }

}
