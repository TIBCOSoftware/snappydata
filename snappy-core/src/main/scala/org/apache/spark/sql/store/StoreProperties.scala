package org.apache.spark.sql.store

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.cluster.SnappyCoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.local.LocalBackend

/**
 * Created by rishim on 30/10/15.
 */
object StoreProperties {

  private val DEFAULT_EMBEDDED_URL = "jdbc:snappydata:;"

  def defaultStoreURL(sc : SparkContext): String = {

   if (sc.master.startsWith("snappydata") || sc.conf.contains("snappydata.store.locators")) {
     DEFAULT_EMBEDDED_URL // Embedded mode. Already connected to Snappydata in embedded , nonembedded and local mode
    } else {
     sys.error("Option 'url' not specified")
    }
  }

}
