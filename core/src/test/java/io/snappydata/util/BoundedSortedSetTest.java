/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package io.snappydata.util;

import java.util.Iterator;

import org.apache.spark.sql.collection.BoundedSortedSet;
import scala.Tuple2;
import junit.framework.TestCase;

public class BoundedSortedSetTest extends TestCase {
  
  public void testBoundedSortedMapTestOrdering() {
    BoundedSortedSet<String, Long> boundedSet = new BoundedSortedSet<>(8, false);
    for(int i = 1; i <= 100; ++i) {
      boundedSet.add(new Tuple2<>(i + "", Long.valueOf(i))) ;
    }
    assertEquals(8,boundedSet.size());
    Iterator<Tuple2<String, Long>> iter = boundedSet.iterator();
    int i = 100;
    while(iter.hasNext()) {
      Tuple2<String, Long> elem = iter.next();
      assertEquals(i+"", elem._1);
      assertEquals(i, elem._2.longValue());
      --i;
    }
    assertEquals(92,i);
  }
  
  public void testBoundedSortedMapTestOrderingReverseInsertion() {
    BoundedSortedSet<String, Long> boundedSet = new BoundedSortedSet<String, Long>(8, false);
    for(int i = 100; i > 0; --i) {
      boundedSet.add(new Tuple2<>(i + "", Long.valueOf(i)));
    }
    assertEquals(8,boundedSet.size());
    Iterator<Tuple2<String, Long>> iter = boundedSet.iterator();
    int i = 100;
    while(iter.hasNext()) {
      Tuple2<String, Long> elem = iter.next();
      assertEquals(i+"", elem._1);
      assertEquals(i, elem._2.longValue());
      --i;
    }
    assertEquals(92,i);
  }
  
  public void testContains() {
    BoundedSortedSet<String, Long> boundedSet = new BoundedSortedSet<String, Long>(8, false);
    for(int i = 100; i > 0; --i) {
      boundedSet.add(new Tuple2<>(i + "", Long.valueOf(i)));
    }
   
    for(int i =100; i  > 92; --i) {
      assertTrue(boundedSet.contains(i+""));
     
     assertEquals(boundedSet.get(i +"").longValue(), i);
     
    }
  }

}
