package org.apache.spark.sql.collection;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import scala.Tuple2;

public class BoundedSortedSet<K, V extends Comparable<V>> extends
    TreeSet<Tuple2<K, V>> {

  private final int bound;
  private final Map<K, V> map;
  private final static float tolerance = 0;
  private final boolean isRelaxedBound;

  public BoundedSortedSet(int bound, boolean isRelaxedBound) {
    super(new Comparator<Tuple2<K, V>>() {
      @Override
      public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
        if (o1._1.equals(o2._1)) {
          return 0;
        } else if (o1._2.compareTo(o2._2) > 0) {
          return -1;
        } else {
          return 1;
        }
      }

    });
    this.bound = bound;
    this.map = new HashMap<K, V>();
    this.isRelaxedBound = isRelaxedBound;
  }

  @Override
  public boolean contains(Object key) {
    return this.map.containsKey(key);
  }

  @Override
  public boolean add(Tuple2<K, V> data) {
    // check if the structure already contains this key
    V prevCount = this.map.get(data._1);
    if (prevCount != null) {
      this.remove(new Tuple2<K, V>(data._1, prevCount));
    }
    boolean added = false;

    super.add(data);
    if (this.size() > this.bound) {
      boolean remove = false;
      if (this.isRelaxedBound) {
        // If relaxed bound is true, data will always be stored as Long
        // value
        // && not approximate value
        Iterator<Tuple2<K, V>> iter = this.descendingIterator();
        Tuple2<K, V> last = iter.next();

        if (iter.hasNext()) {
          Tuple2<K, V> last_1 = iter.next();
          if ((Math.abs((Long) last._2 - (Long) last_1._2) * 100f)
              / (Long) last_1._2 > tolerance) {
            remove = true;
          }
        }
      } else {
        remove = true;
      }

      if (remove) {
        Tuple2<K, V> prev = this.pollLast();
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
    } else {
      this.map.put(data._1, data._2);
      added = true;
    }

    return added;
  }

  public V get(K key) {
    return this.map.get(key);
  }

  public int getBound() {
    return this.bound;
  }

}
