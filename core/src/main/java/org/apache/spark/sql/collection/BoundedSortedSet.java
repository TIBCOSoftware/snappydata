/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.collection;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
    this.map = new HashMap<>();
    this.isRelaxedBound = isRelaxedBound;
  }

  @Override
  public boolean contains(Object key) {
    return this.map.containsKey(key);
  }

  private void addBypassCheck(K key, V value) {
    this.map.put(key, value);
    super.add(new Tuple2<K, V>(key, value));
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
        //Tuple2<K, V> last = iter.next();
        Tuple2<K, V> last = iter.next();
        if (iter.hasNext()) {
          Tuple2 last_1 = iter.next();
          if ((Math.abs((Long)last._2 - (Long)last_1._2) * 100f)
              / (Long)last_1._2 > tolerance) {
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

  public void serialize(DataOutputStream dos) throws IOException {
    dos.writeInt(this.bound);
    dos.writeBoolean(this.isRelaxedBound);
    dos.writeInt(this.map.size());
    ObjectOutputStream oos = null;
    boolean firstCall = true;
    boolean isString = false;
    for (Map.Entry<K, V> entry : this.map.entrySet()) {
      if (firstCall) {
        if (entry.getKey() instanceof String) {
          isString = true;
        } else {
          oos = new ObjectOutputStream(dos);
        }
        dos.writeBoolean(isString);
        firstCall = false;
      }
      if (isString) {
        dos.writeUTF((String)entry.getKey());
        dos.writeLong((Long)entry.getValue());
      } else {
        oos.writeObject(entry.getKey());
        oos.writeLong((Long)entry.getValue());
      }
    }
    if (oos != null) {
      oos.flush();
    }
  }

  @SuppressWarnings("unchecked")
  public static <K, V extends Comparable<V>> BoundedSortedSet<K, V> deserialize(
      DataInputStream dis) throws Exception {
    int bound = dis.readInt();
    boolean relaxedBound = dis.readBoolean();
    BoundedSortedSet<K, V> bs = new BoundedSortedSet<>(bound, relaxedBound);
    int mapSize = dis.readInt();

    if (mapSize > 0) {
      boolean isString = dis.readBoolean();
      ObjectInputStream ois = null;
      if (!isString) {
        ois = new ObjectInputStream(dis);
      }
      for (int i = 0; i < mapSize; ++i) {
        if (isString) {
          String key = dis.readUTF();
          Long value = dis.readLong();
          bs.addBypassCheck((K)key, (V)value);

        } else {
          Object key = ois.readObject();
          Long value = ois.readLong();
          bs.addBypassCheck((K)key, (V)value);
        }
      }
    }
    return bs;
  }
}
