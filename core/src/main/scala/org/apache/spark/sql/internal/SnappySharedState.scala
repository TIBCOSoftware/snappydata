/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.internal

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.{HiveClientUtil, SnappyExternalCatalog}

/**
 * Right now we are not overriding anything from Spark's built in SharedState. We need to
 * re-visit if we need this at all.
 *
 */
private[sql] class SnappySharedState(override val sparkContext: SparkContext)
    extends SharedState(sparkContext) {

  /**
   * A Hive client used to interact with the metastore.
   */
  lazy val metadataHive = new HiveClientUtil(sparkContext).client


  override lazy val externalCatalog =
    new SnappyExternalCatalog(metadataHive, sparkContext.hadoopConfiguration)
}
