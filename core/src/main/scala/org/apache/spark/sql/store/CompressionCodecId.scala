/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

package org.apache.spark.sql.store

import io.snappydata.Constant

import org.apache.spark.sql.collection.Utils

/**
 * Compression schemes supported by snappy-store.
 */
object CompressionCodecId extends Enumeration {
  type Type = Value

  val LZ4_ID = 1
  val SNAPPY_ID = 2

  val None = Value(0, "None")
  val LZ4 = Value(LZ4_ID, "LZ4")
  val Snappy = Value(SNAPPY_ID, "Snappy")

  def fromName(name: String): CompressionCodecId.Type =
    if (name eq null) Constant.DEFAULT_CODECID
    else Utils.toLowerCase(name) match {
      case "lz4" => LZ4
      case "snappy" => Snappy
      case "none" | "uncompressed" => None
      case _ => throw new IllegalArgumentException(
        s"Unknown compression scheme '$name'")
    }
}
