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

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A cyclic barrier for use in synchronizing between dunit VMs. Each
 * thread calling await will wait for <code>parties</code> threads to call
 * await, at which point all parties will proceed.
 */
public class AnyCyclicBarrier {

  private static final String KEY_PREFIX = "DUNIT_CYCLIC_BARRIER_";

  private final DUnitBB bb;
  private final int parties;
  private final String keyName;
  private final AtomicInteger awaitRound;

  public AnyCyclicBarrier(int parties, String lockName) {
    this.bb = DUnitBB.getBB();
    this.parties = parties;
    this.keyName = KEY_PREFIX + lockName;
    this.awaitRound = new AtomicInteger(1);
  }

  public void await() throws InterruptedException, BrokenBarrierException {
    try {
      await(-1L);
    } catch (TimeoutException te) {
      throw new AssertionError("unexpected timeout exception", te);
    }
  }

  public void await(final long timeoutInMillis) throws InterruptedException,
      BrokenBarrierException, TimeoutException {
    if (this.parties <= 1) {
      return;
    }

    final long start = System.currentTimeMillis();
    int waiters = this.bb.addAndGet(this.keyName, 1, 1);
    while (waiters < (this.parties * this.awaitRound.get())) {
      if (timeoutInMillis >= 0 &&
          (System.currentTimeMillis() - start) > timeoutInMillis) {
        throw new TimeoutException("exceeded timeout of " + timeoutInMillis
            + "ms waiting on barrier");
      }
      Thread.sleep(10);
      Object waitersObj = this.bb.get(this.keyName);
      if (waitersObj == null) {
        throw new BrokenBarrierException("Barrier broken: object destroyed");
      }
      waiters = (Integer)waitersObj;
    }
    this.awaitRound.incrementAndGet();
  }

  public void destroy() {
    this.bb.remove(this.keyName);
  }

  public static void destroy(String lockName) {
    DUnitBB.getBB().remove(KEY_PREFIX + lockName);
  }
}
