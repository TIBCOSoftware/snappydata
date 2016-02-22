/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import java.util.concurrent.TimeoutException;

/**
 * A cyclic barrier for use in synchronizing between dunit VMs. Each
 * thread calling await will wait for <code>parties</code> threads to call
 * await, at which point all parties will proceed.
 */
public class AnyCyclicBarrier {

  private static final String PARTIES_KEY = "parties";
  private final int parties;
  private final DUnitBB bb;

  public AnyCyclicBarrier(int parties) {
    bb = DUnitBB.getBB();
    bb.put(PARTIES_KEY, parties);
    this.parties = parties;
  }

  public void await() throws InterruptedException {
    if (parties <= 1) {
      bb.remove(PARTIES_KEY);
      return;
    }

    int remaining = bb.addAndGet(PARTIES_KEY, -1, parties - 1);
    if (remaining > 0) {
      Integer waiters;
      while ((waiters = (Integer)bb.get(PARTIES_KEY)) != null && waiters > 0) {
        Thread.sleep(10);
      }
    }
    bb.remove(PARTIES_KEY);
  }

  public void await(final long timeoutInMillis) throws InterruptedException,
      TimeoutException {
    if (parties <= 1) {
      bb.remove(PARTIES_KEY);
      return;
    }

    final long start = System.currentTimeMillis();
    int remaining = bb.addAndGet(PARTIES_KEY, -1, parties - 1);
    if (remaining > 0) {
      Integer waiters;
      while ((waiters = (Integer)bb.get(PARTIES_KEY)) != null && waiters > 0) {
        if (timeoutInMillis >= 0 &&
            (System.currentTimeMillis() - start) > timeoutInMillis) {
          throw new TimeoutException("exceeded timeout of " + timeoutInMillis
              + "ms waiting on barrier");
        }
        Thread.sleep(10);
      }
    }
    bb.remove(PARTIES_KEY);
  }
}
