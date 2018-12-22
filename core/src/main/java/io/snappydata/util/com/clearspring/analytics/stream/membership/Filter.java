/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package io.snappydata.util.com.clearspring.analytics.stream.membership;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import io.snappydata.util.StringUtils;
import io.snappydata.util.com.clearspring.analytics.hash.MurmurHash;

public abstract class Filter {

    int hashCount;

    public int getHashCount() {
        return hashCount;
    }

    public int[] getHashBuckets(String key, boolean applyWidth) {
        return Filter.getHashBuckets(key, hashCount, buckets(), applyWidth);
    }

    public int[] getHashBuckets(byte[] key, boolean applyWidth) {
        return Filter.getHashBuckets(key, hashCount, buckets(), applyWidth);
    }


    abstract int buckets();

    public abstract void add(String key);

    public abstract boolean isPresent(String key);

    // for testing
    abstract int emptyBuckets();

    @SuppressWarnings("unchecked")
    ICompactSerializer<Filter> getSerializer() {
        Method method = null;
        try {
            method = getClass().getMethod("serializer");
            return (ICompactSerializer<Filter>) method.invoke(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static final int seed1 = 0x3c074a61;
    static final int seed2 = 0xf7ca7fd2;

    // Murmur is faster than an SHA-based approach and provides as-good collision
    // resistance.  The combinatorial generation approach described in
    // https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
    // does prove to work in actual tests, and is obviously faster
    // than performing further iterations of murmur.
    public static int[] getHashBuckets(String key, int hashCount, int max, boolean applyWidth) {
        byte[] b;
        b = key.getBytes(StandardCharsets.UTF_8);
        return getHashBuckets(b, hashCount, max, applyWidth);
    }

    static int[] getHashBuckets(byte[] b, int hashCount, int max, boolean applyWidth) {
        int[] result = new int[hashCount];
        int hash1 = MurmurHash.hash(b, b.length, seed1);
        int hash2 = MurmurHash.hash(b, b.length, hash1 * seed2);
        for (int i = 0; i < hashCount; i++) {
        	if(applyWidth) {
              result[i] = Math.abs((hash1 + i * hash2) % max);
        	}else {
              result[i] = Math.abs(hash1 + i * hash2 );
        	}
        }
        return result;
    }
}
