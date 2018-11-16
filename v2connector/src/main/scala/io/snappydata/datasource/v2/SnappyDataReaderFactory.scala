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

package io.snappydata.datasource.v2

import java.io.IOException

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}

class SnappyDataReaderFactory extends DataReaderFactory[Row] with DataReader[Row] {
  /**
   * Returns a data reader to do the actual reading work.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  override def createDataReader(): DataReader[Row] = {

    this
  }

  /**
   * Proceed to next record, returns false if there is no more records.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   *
   * @throws IOException if failure happens during disk/network IO like reading files.
   */
  override def next(): Boolean = {
    // For the first cut we are assuming that we will get entire data
    // in the first call of this method
    // We will have to think about breaking into chunks if the data size
    // too huge to handle in one fetch
    // Check the current smart connector code, to see how row buffers and column
    // batches are brought and how filters and column projections are pushed.
    // We can exactly mirror the smart connector implementation
    // We decode and form a row. We will use our decoder classes
    // which are to be moved to a new package. This package needs to be present in
    // the classpath
    false
  }

  /**
   * Return the current record. This method should return same value until `next` is called.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  override def get(): Row = {
    // Return the row decoded in next()
    // fake return value just to be able compile
    Row.fromSeq(Seq(1))
  }

  override def close(): Unit = {}
}