package io.snappydata.util

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
  * Utility functions for working with Typesafe Config.
  */
object ConfigUtils {

  /**
    * Return the default config object, but with $USER.conf layered on top.
    * This allows each of the developers to have their own private overrides,
    * for config values, yet share a common config.
    *
    *  So, if the following files are in /resources dir:
    *
    *    application.conf  mary.conf
    *
    *  and user mary runs the program, then she will get a config with any values in
    *  mary.conf overriding those in application.conf.  If bob runs the program, he
    *  will get only the values in application.conf, since there is no bob.conf.
    */
  def getUserConfig(): Config = {
    val user = System.getenv("USER").toLowerCase
    val appconfig = ConfigFactory.load()
    val userconfig = ConfigFactory.load(user)
    if (userconfig == null) {
      appconfig
    } else {
      userconfig.withFallback(appconfig)
    }
  }
}
