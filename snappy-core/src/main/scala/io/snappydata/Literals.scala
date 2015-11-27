package io.snappydata

/**
 * Created by soubhikc on 11/11/15.
 *
 * Constant names suggested per naming convention
 * http://docs.scala-lang.org/style/naming-conventions.html
 *
 * we decided to use upper case with underscore word separator.
 */
object Constant {

  val DEFAULT_EMBEDDED_URL = "jdbc:snappydata:"

  val JDBC_URL_PREFIX = "snappydata://"

  val JDBC_EMBEDDED_DRIVER = "com.pivotal.gemfirexd.jdbc.EmbeddedDriver"

  val JDBC_CLIENT_DRIVER = "com.pivotal.gemfirexd.jdbc.ClientDriver"

  val PROPERTY_PREFIX = "snappydata."

  val STORE_PROPERTY_PREFIX = s"${PROPERTY_PREFIX}store."

  private[snappydata] val JOBSERVER_PROPERTY_PREFIX = "jobserver."

  val DEFAULT_SCHEMA = "APP"
}

/**
 * Property names should be as per naming convention
 * http://docs.scala-lang.org/style/naming-conventions.html
 * i.e. upper camel case.
 */
object Property {

  val locators = s"${Constant.STORE_PROPERTY_PREFIX}locators"

  val mcastPort = s"${Constant.STORE_PROPERTY_PREFIX}mcast-port"

  val jobserverEnabled = s"${Constant.JOBSERVER_PROPERTY_PREFIX}enabled"

  val jobserverConfigFile = s"${Constant.JOBSERVER_PROPERTY_PREFIX}configFile"

  val embedded = s"${Constant.PROPERTY_PREFIX}embedded"

  val metastoreDBURL = s"${Constant.PROPERTY_PREFIX}metastore-db-url"

  val metastoreDriver = s"${Constant.PROPERTY_PREFIX}metastore-db-driver"
}
