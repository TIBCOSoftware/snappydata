package org.apache.spark.sql.store

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.local.LocalBackend

/**
 * Created by rishim on 30/10/15.
 */
object StoreProperties {

  val SNAPPY_STORE_JDBC_URL = "snappy.store.jdbc.url"

  def defaultStoreURL(sc : SparkContext): String = {
/*    if (sc.master.startsWith("snappydata://") || sc.conf.contains("snappy.locators")) {
      // embedded mode
      "jdbc:snappydata:;"
    } else {

    }*/
    sc.schedulerBackend match {
      case lb: LocalBackend => "jdbc:snappydata:;mcast-port=0;"
      case _ => "jdbc:snappydata:"
    }
  }

}
