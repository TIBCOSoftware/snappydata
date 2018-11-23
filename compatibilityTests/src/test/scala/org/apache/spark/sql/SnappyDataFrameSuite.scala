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

import org.apache.spark.sql.test.{SharedSnappySessionContext, SnappySparkTestUtil}

class SnappyDataFrameSuite extends DataFrameSuite
    with SharedSnappySessionContext with SnappySparkTestUtil {

  override def ignored: Seq[String] = Seq(
    "dataframe toString",
    "withColumn",
    "drop column using drop",
    "drop unknown column (no-op)",
    "drop column using drop with column reference",
    "drop unknown column (no-op) with column reference",
    "drop unknown column with same name with column reference",
    "drop column after join with duplicate columns using column reference",
    "withColumnRenamed",
    "showString: truncate = [0, 20]",
    "showString: truncate = [3, 17]",
    "showString(negative)",
    "showString(0)",
    "SPARK-7319 showString",
    "SPARK-7327 show with empty dataFrame",
    "sameResult() on aggregate",
    "SPARK-17409: Do Not Optimize Query in CTAS (Data source tables) More Than Once"
  )
}
