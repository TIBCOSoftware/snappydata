package org.apache.spark.sql.internal

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, ExternalCatalog}


private[sql] class SnappySharedState(override val sparkContext: SparkContext)
      extends SharedState(sparkContext) with Logging {

  /**
   * A catalog that interacts with external systems.
   */
  lazy override val externalCatalog: ExternalCatalog = new InMemoryCatalog(sparkContext
      .hadoopConfiguration)

}
