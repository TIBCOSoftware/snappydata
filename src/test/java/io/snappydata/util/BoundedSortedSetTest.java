package io.snappydata.util;

import java.util.Iterator;

import org.apache.spark.sql.collection.BoundedSortedSet;
import scala.Tuple2;
import junit.framework.TestCase;

public class BoundedSortedSetTest extends TestCase {
  
  public void testBoundedSortedMapTestOrdering() {
    BoundedSortedSet<String> boundedSet = new BoundedSortedSet<String>(8);
    for(int i = 1; i <= 100; ++i) {
      boundedSet.add(new Tuple2(i+"", Long.valueOf(i))) ;
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
    BoundedSortedSet<String> boundedSet = new BoundedSortedSet<String>(8);
    for(int i = 100; i > 0; --i) {
      boundedSet.add(new Tuple2<String,Long>(i+"", Long.valueOf(i) ));
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
    BoundedSortedSet<String> boundedSet = new BoundedSortedSet<String>(8);
    for(int i = 100; i > 0; --i) {
      boundedSet.add(new Tuple2<String,Long>(i+"", Long.valueOf(i)));
    }
   
    for(int i =100; i  > 92; --i) {
      assertTrue(boundedSet.contains(i+""));
     
     assertEquals(boundedSet.get(i +"").longValue(), i);
     
    }
  }

}
