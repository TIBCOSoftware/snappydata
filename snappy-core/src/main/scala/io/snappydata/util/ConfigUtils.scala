/*
 * Copyright (c) 2010-2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.util


import _root_.com.typesafe.config.Config
import _root_.com.typesafe.config.ConfigFactory

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
