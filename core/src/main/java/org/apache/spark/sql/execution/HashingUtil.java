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

  private static final int C1 = 0xcc9e2d51;
  private static final int C2 = 0x1b873593;

  // Simpler mixing for integer values that are reasonably hashed.
  // Constant taken from h2 tests (CalculateHashConstant.java).
  public static int hashInt(int v) {
    v = ((v >>> 16) ^ v) * 0x45d9f3b;
    v = ((v >>> 16) ^ v) * 0x45d9f3b;
    v = (v >>> 16) ^ v;
    return v;
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
