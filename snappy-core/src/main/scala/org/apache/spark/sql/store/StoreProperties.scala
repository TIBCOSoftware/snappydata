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

   if (sc.master.startsWith("snappydata://") || sc.conf.contains("snappy.locators")) {
     DEFAULT_EMBEDDED_URL // Embedded mode. Take a peer connection
    } else {
     sc.schedulerBackend match {
       case lb: LocalBackend => DEFAULT_EMBEDDED_URL //TDOD need to check with Soubhick for local mode
       case lb: SnappyCoarseGrainedSchedulerBackend => DEFAULT_EMBEDDED_URL
       case _ => sys.error("Option 'url' not specified")
     }
    }
  }

}
