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
package io.snappydata.test.dunit.standalone;

import java.rmi.RemoteException;
import java.util.Map;

import io.snappydata.test.util.TestException;

/**
 * A blackboard for DUnit tests that can be used to share information
 * between the JVMs that are part of a DUnit test.
 */
public class DUnitBB {

  protected DUnitBB() {
  }

  private static final DUnitBB instance = new DUnitBB();

  /**
   * Get the DUnitBB
   */
  public static DUnitBB getBB() {
    return instance;
  }

  public Object get(Object key) {
    try {
      return DUnitLauncher.getMaster().get(key);
    } catch (RemoteException e) {
      throw new TestException(e.toString(), e);
    }
  }

  public boolean put(Object key, Object value) {
    try {
      return DUnitLauncher.getMaster().put(key, value);
    } catch (RemoteException e) {
      throw new TestException(e.toString(), e);
    }
  }

  public boolean remove(Object key) {
    try {
      return DUnitLauncher.getMaster().remove(key);
    } catch (RemoteException e) {
      throw new TestException(e.toString(), e);
    }
  }

  public int addAndGet(Object key, int delta, int defaultValue) {
    try {
      return DUnitLauncher.getMaster().addAndGet(key, delta, defaultValue);
    } catch (RemoteException e) {
      throw new TestException(e.toString(), e);
    }
  }

  public Map<Object, Object> getMapCopy() {
    try {
      return DUnitLauncher.getMaster().getMapCopy();
    } catch (RemoteException e) {
      throw new TestException(e.toString(), e);
    }
  }

  public void acquireSharedLock() {
    try {
      DUnitLauncher.getMaster().acquireSharedLock();
    } catch (RemoteException e) {
      throw new TestException(e.toString(), e);
    }
  }

  public void releaseSharedLock() {
    try {
      DUnitLauncher.getMaster().releaseSharedLock();
    } catch (RemoteException e) {
      throw new TestException(e.toString(), e);
    }
  }
}
