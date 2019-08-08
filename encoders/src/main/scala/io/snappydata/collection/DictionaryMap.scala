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

package io.snappydata.collection

import com.gemstone.gemfire.internal.shared.BufferAllocator

import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

/**
 * An extension of [[ByteBufferHashMap]] to store string keys. The dictionary
 * index is stored as part of key bytes while the variable length string value
 * is stored in the value bytes.
 */
final class DictionaryMap(_initialCapacity: Int, _loadFactor: Double,
    _valueSize: Int, _allocator: BufferAllocator, _keyData: ByteBufferData = null,
    _valueData: ByteBufferData = null, _valueDataPosition: Long = 0L)
    extends ByteBufferHashMap(_initialCapacity, _loadFactor, 4 /* for dictionary index */ ,
      _valueSize, _allocator, _keyData, _valueData, _valueDataPosition) {

  /**
   * Add a new string to the map for dictionaries. The key field has the
   * index of the value i.e. (n - 1) for nth distinct string added to the map,
   * with the offset into the value. The string itself is stored back to back
   * in the value portion with its size at the start being variable length.
   * This exactly matches the end format of the dictionary encoding that
   * stores the dictionary string back-to-back in index order and expected
   * by DictionaryDecoders. So the encoder can use the final
   * value serialized array as is for putting into the encoded column batch
   * (followed by the dictionary indexes of actual values themselves).
   *
   * The encoded values are read in the initialization of DictionaryDecoder
   * and put into an array, and looked up by its readUTF8String method.
   */
  def addDictionaryString(key: UTF8String): Int = {
    val numBytes = key.numBytes()
    putBufferIfAbsent(key.getBaseObject, key.getBaseOffset, numBytes, numBytes,
      key.hashCode())
  }

  override protected def handleExisting(mapKeyObject: AnyRef, mapKeyOffset: Long): Int = {
    // get the dictionary index from the key bytes
    Platform.getInt(mapKeyObject, mapKeyOffset + 8)
  }

  override protected def handleNew(mapKeyObject: AnyRef, mapKeyOffset: Long): Int = {
    val newIndex = size
    // write the dictionary index in the key bytes
    Platform.putInt(mapKeyObject, mapKeyOffset + 8, newIndex)
    handleNewInsert()
    // return negative of index to indicate insert
    -newIndex - 1
  }

  def duplicate(): DictionaryMap = {
    new DictionaryMap(capacity - 1, loadFactor, valueSize, allocator,
      keyData.duplicate(), valueData.duplicate(), valueData.baseOffset)
  }
}
