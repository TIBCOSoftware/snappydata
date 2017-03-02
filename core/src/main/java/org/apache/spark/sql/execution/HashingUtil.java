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
package org.apache.spark.sql.execution;

/**
 * Helper class with static methods used by <code>ObjectHashMapAccessor</code>.
 */
public abstract class HashingUtil {

  /** 2<sup>32</sup> &middot; &phi;, &phi; = (&#x221A;5 &minus; 1)/2. */
  private static final int INT_PHI = 0x9E3779B9;

  private static final int C1 = 0xcc9e2d51;
  private static final int C2 = 0x1b873593;

  /** Quickly mixes the bits of an integer.
   *
   * <p>This method mixes the bits of the argument by multiplying by the
   * golden ratio and XORshifting the result. It is borrowed from
   * <a href="https://github.com/OpenHFT/Koloboke">Koloboke</a>, and
   * it has slightly worse behaviour than MurmurHash3 (in open-addressed
   * hash tables the average number of probes is slightly larger),
   * but it's much faster.
   */
  public static int hashInt(final int v) {
    final int h = v * INT_PHI;
    return h ^ (h >>> 16);
  }

  public static int hashLong(long v) {
    return hashInt((int)(v ^ (v >>> 32)));
  }

  // Public Murmur3 methods for invocation by generated code.

  public static int mixK1(int k1) {
    k1 *= C1;
    k1 = (k1 << 15) | (k1 >>> -15); // rotateLeft
    k1 *= C2;
    return k1;
  }

  public static int mixH1(int h1, final int k1) {
    h1 ^= k1;
    h1 = (h1 << 13) | (k1 >>> -13); // rotateLeft
    h1 = h1 * 5 + 0xe6546b64;
    return h1;
  }

  public static int finalMix(int h1, final int length) {
    h1 ^= length;
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;
    return h1;
  }
}
