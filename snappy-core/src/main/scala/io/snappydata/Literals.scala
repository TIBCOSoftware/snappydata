package io.snappydata

import org.apache.spark.scheduler.cluster.SnappyEmbeddedModeClusterManager

/**
  * Created by soubhikc on 11/11/15.
  *
  * Constant names should be as per naming convention
  * http://docs.scala-lang.org/style/naming-conventions.html
  * i.e. upper camel case.
  */
final object Const {
  SnappyEmbeddedModeClusterManager

  val DEFAULT_EMBEDDED_URL = "jdbc:snappydata:;"

  val jdbcUrlPrefix = "snappydata://";

  private[snappydata] val propPrefix = "snappydata."

}

/**
  * Created by soubhikc on 11/11/15.
  *
  * Property names should be as per naming convention
  * http://docs.scala-lang.org/style/naming-conventions.html
  * i.e. upper camel case.
  */
final object Prop {
  SnappyEmbeddedModeClusterManager

  val locators = s"${Const.propPrefix}locators"

  val jobserverEnabled = s"jobserver.enabled"

  val jobserverConfigFile = "jobserver.configFile"
}
