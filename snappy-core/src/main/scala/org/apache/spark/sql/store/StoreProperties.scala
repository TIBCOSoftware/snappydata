package org.apache.spark.sql.store

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.local.LocalBackend

/**
 * Created by rishim on 30/10/15.
 */
object StoreProperties {

  val SNAPPY_STORE_JDBC_URL = "snappy.store.jdbc.url"

  def defaultStoreURL(sc : SparkContext): String = {
    sc.schedulerBackend match {
      case lb: LocalBackend => "jdbc:gemfirexd:;mcast-port=0;"//TODO need to change  com.pivotal.gemfirexd.TestUtil to make it snappydata
      case _ => "jdbc:gemfirexd:"
    }
  }

}
