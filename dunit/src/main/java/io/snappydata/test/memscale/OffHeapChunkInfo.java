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
package io.snappydata.test.memscale;

/**
 * @author lynng
 * 
 * A "chunk" is a segment of off-heap memory that stores one object. Instances of this class
 * track information about a chunk to be used for validation.
 *
 */
public class OffHeapChunkInfo implements Comparable {
  
  private String regionName = null;
  private Object key = null;
  private long firstMemoryAddress = -1;
  private long numberBytes = -1;
  
  public OffHeapChunkInfo(String regionNameArg, Object keyArg, long memoryAddressArg, long numberBytesArg) {
    regionName = regionNameArg;
    key = keyArg;
    firstMemoryAddress = memoryAddressArg;
    numberBytes = numberBytesArg;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (firstMemoryAddress ^ (firstMemoryAddress >>> 32));
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof OffHeapChunkInfo)) {
      return false;
    }
    OffHeapChunkInfo other = (OffHeapChunkInfo) obj;
    if (firstMemoryAddress != other.firstMemoryAddress) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(Object o) {
    if (this.equals(o)) {
      return 0;
    }
    OffHeapChunkInfo other = (OffHeapChunkInfo) o;
    if (this.firstMemoryAddress < other.firstMemoryAddress) {
      return -1;
    }
    return 1;
  }
  
  /** Return the last memory address consumed by this chunk
   * 
   * @return The last memory address consumed by this chunk.
   */
  public long getLastMemoryAddress() {
    return firstMemoryAddress + numberBytes - 1;
  }
  
  /**
   * @return the regionName that has an entry whose value is in this chunk
   */
  public String getRegionName() {
    return regionName;
  }

  /**
   * @return the key whose value is in this chunk
   */
  public Object getKey() {
    return key;
  }

  /**
   * @return the first memory address consumed by this chunk
   */
  public long getFirstMemoryAddress() {
    return firstMemoryAddress;
  }

  /**
   * @return the numberBytes consumed by this chunk
   */
  public long getNumberBytes() {
    return numberBytes;
  }

  public String toString() {
    return "Off-heap memory for key " + getKey() + " in " + getRegionName() + " starts at " + getFirstMemoryAddress() +
        " and ends at " + getLastMemoryAddress() + " (span of " + getNumberBytes() + " bytes)";
  }
}
