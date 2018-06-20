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
package org.apache.spark.sql.execution.columnar.encoding

import io.snappydata.SnappyFunSuite

import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.unsafe.types.UTF8String


class ColumnEncoderSuite extends SnappyFunSuite {

  test("encoding with int type") {
    val snc = new SnappySession(sc)
    snc.sql("create table t1(id int) using column options ()")
    val df = snc.table("t1")
    val encoders = df.schema.fields.map(f =>
      ColumnEncoding.getColumnEncoder(f))
    assert(encoders.length == 1)
    assert(encoders(0).isInstanceOf[UncompressedEncoderNullable])
  }

  test("encoding with String type with more than Short.MaxValue nulls") {
    val f = new StructField("a", StringType, nullable = true)
    val encoder = ColumnEncoding.getColumnEncoder(f)
    assert(encoder.isInstanceOf[DictionaryEncoderNullable])
    // Need to initialize the encoder
    var cursor = encoder.initialize(f, 100000, true)

    cursor = encoder.writeUTF8String(cursor, UTF8String.fromString("abc"))
    cursor = encoder.writeUTF8String(cursor, UTF8String.fromString("pqr"))
    (2 to Short.MaxValue + 4).map(i => encoder.writeIsNull(i))
    cursor = encoder.writeUTF8String(cursor, UTF8String.fromString("mnl"))
    cursor = encoder.writeUTF8String(cursor, UTF8String.fromString("zbs"))
    cursor = encoder.writeUTF8String(cursor, UTF8String.fromString("abc"))

    val buffer = encoder.finish(cursor)
    encoder.close()
    val decoder = ColumnEncoding.getColumnDecoder(buffer, f)
    val arr = buffer.array()
    val ordinal = 0
    var numNulls = 0

    def incNumNulls(): Int = {
      numNulls += 1
      numNulls
    }
    var str = ""
    for (ordinal <- 0 to Short.MaxValue + 7) {
      // The following logic is used to scan encoded data in ColumnTableScan.
      // (See ColumnTableScan.genIfNonNullCode)
      // Replicating the same logic here for unit test
      val nextNullposition = decoder.getNextNullPosition
      if (ordinal < nextNullposition ||
          (ordinal == nextNullposition + 1 &&
              ordinal < decoder.findNextNullPosition(arr, nextNullposition,
                incNumNulls)) ||
          (ordinal != nextNullposition &&
              ((numNulls = decoder.numNulls(arr, ordinal, numNulls)) == 0) ||
              ordinal != decoder.getNextNullPosition)
      ) {

        str = decoder.readUTF8String(arr, ordinal - numNulls).toString
      } else {
        // println("null handling")
      }
    }
    assert(str.equals("abc"))
  }
}

