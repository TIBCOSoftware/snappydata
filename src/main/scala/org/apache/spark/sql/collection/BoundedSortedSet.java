package org.apache.spark.sql.collection;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import scala.Tuple2;

public class BoundedSortedSet<T> extends TreeSet<Tuple2<T, Long>> {

  private final int bound;
  private final Map<T, Long> map;

  public BoundedSortedSet(int bound) {
    super(new Comparator<Tuple2<T, Long>>() {

      @Override
      public int compare(Tuple2<T, Long> o1, Tuple2<T, Long> o2) {
        if (o1._1.equals(o2._1)) {
          return 0;
        } else if (o1._2 > o2._2) {
          return -1;
        } else {
          return 1;
        }
      }

    });
    this.bound = bound;
    this.map = new HashMap<T, Long>();
  }
  
  
 
  
  @Override
  public boolean contains(Object key) {
    return this.map.containsKey(key);
  }

  @Override
  public boolean add(Tuple2<T, Long> data) {
    // check if the structure already contains this key
    Long prevCount = this.map.get(data._1);
    if (prevCount != null) {
      this.remove(new Tuple2(data._1, prevCount));
    }
    boolean added = false;

    super.add(data);
    if (this.size() > bound) {
      Tuple2<T, Long> prev = this.pollLast();
      if (!prev._1.equals(data._1)) {
        this.map.put(data._1, data._2);
        this.map.remove(prev._1);
        added = true;
      } else {
        if (prevCount != null) {
          this.map.remove(data._1);
        }
      }
    } else {
      this.map.put(data._1, data._2);
      added = true;
    }

    return added;
  }
  
  public Long get(T key) {
    return this.map.get(key);
  }

}
