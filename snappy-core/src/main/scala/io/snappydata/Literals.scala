package io.snappydata

import org.apache.spark.scheduler.cluster.SnappyEmbeddedModeClusterManager

/**
  * Created by soubhikc on 11/11/15.
  *
  * Constant names sugged per naming convention
  * http://docs.scala-lang.org/style/naming-conventions.html
  * we decided to use upper case with underscore word separator.
  */
object Constant {
  SnappyEmbeddedModeClusterManager.register

  val DEFAULT_EMBEDDED_URL = "jdbc:snappydata:;"

  val JDBC_URL_PREFIX = "snappydata://";

  private[snappydata] val PROPERTY_PREFIX = "snappydata."

}

/**
  * Created by soubhikc on 11/11/15.
  *
  * Property names should be as per naming convention
  * http://docs.scala-lang.org/style/naming-conventions.html
  * i.e. upper camel case.
  */
object Property {
  SnappyEmbeddedModeClusterManager.register

  val locators = s"${Constant.PROPERTY_PREFIX}locators"

  val mcastPort = s"${Constant.PROPERTY_PREFIX}mcast-port"

  val jobserverEnabled = s"jobserver.enabled"

  val jobserverConfigFile = "jobserver.configFile"
}
