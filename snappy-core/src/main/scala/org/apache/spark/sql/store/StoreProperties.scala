package org.apache.spark.sql.store

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.cluster.SnappyCoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.local.LocalBackend

/**
 * Created by rishim on 30/10/15.
 */
object StoreProperties {


  def defaultStoreURL(sc : SparkContext): String = {


   if (sc.master.startsWith("snappydata://") || sc.conf.contains("snappy.locators")) {
      "jdbc:snappydata:;" // Embedded mode. Take a peer connection
    } else {
     sc.schedulerBackend match {
       case lb: LocalBackend => "jdbc:snappydata:;" //TDOD need to check with Soubhick for local mode
       case _ => sys.error("Option 'url' not specified")
     }
    }
  }

}
