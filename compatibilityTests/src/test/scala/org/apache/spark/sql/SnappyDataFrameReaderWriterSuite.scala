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
package org.apache.spark.sql

import org.apache.spark.sql.test.{DataFrameReaderWriterSuite, SharedSnappySessionContext, SnappySparkTestUtil}

class SnappyDataFrameReaderWriterSuite
    extends DataFrameReaderWriterSuite
        with SharedSnappySessionContext with SnappySparkTestUtil {

  override def ignored: Seq[String] = Seq(
    "saveAsTable with mode Append should not fail if the table not exists but a same-name temp view exist",
    "saveAsTable with mode Append should not fail if the table already exists and a same-name temp view exist",
    "saveAsTable with mode ErrorIfExists should not fail if the table not exists but a same-name temp view exist",
    "saveAsTable with mode Overwrite should not drop the temp view if the table not exists but a same-name temp view exist",
    "saveAsTable with mode Overwrite should not fail if the table already exists and a same-name temp view exist",
    "saveAsTable with mode Ignore should create the table if the table not exists but a same-name temp view exist",
    "SPARK-18899: append to a bucketed table using DataFrameWriter with mismatched bucketing",
    "SPARK-18912: number of columns mismatch for non-file-based data source table",
    "SPARK-18913: append to a table with special column names"
  )
}
