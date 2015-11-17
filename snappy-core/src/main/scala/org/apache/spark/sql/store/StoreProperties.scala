package org.apache.spark.sql.store

import io.snappydata.{Property, Constant}

import org.apache.spark.SparkContext

/**
  * Created by rishim on 30/10/15.
  */
object StoreProperties {


  def defaultStoreURL(sc: SparkContext): String = {

    // [soubhik] shouldn't this check be Const.JdbcUrlPrefix ?
    // like in SnappyEmbeddedModeClusterManager
    if (sc.master.startsWith("snappydata") ||
        sc.conf.contains(Property.locators) ||
        sc.conf.contains(Property.mcastPort)
    ) {
      // Embedded mode. Already connected to Snappydata in embedded or
      // nonEmbedded or local mode
      Constant.DEFAULT_EMBEDDED_URL
    } else {
      sys.error("Option 'url' not specified")
    }
  }

}
