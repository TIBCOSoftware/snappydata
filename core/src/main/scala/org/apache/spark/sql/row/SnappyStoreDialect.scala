/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.row

import java.util.Properties
import java.util.regex.Pattern

import com.pivotal.gemfirexd.Attribute
import io.snappydata.Constant
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.{SnappyDataBaseDialect, SnappyDataPoolDialect, SnappyStoreClientDialect}

/**
 * Default dialect for GemFireXD >= 1.4.0.
 * Contains specific type conversions to and from Spark SQL catalyst types.
 */
@DeveloperApi
case object SnappyStoreDialect extends SnappyDataBaseDialect {

  // register the dialect
  JdbcDialects.registerDialect(SnappyStoreDialect)

  private val EMBEDDED_REGEX = s"^(${Constant.DEFAULT_EMBEDDED_URL}|${Attribute.PROTOCOL})"
  private val EMBEDDED_PATTERN = Pattern.compile(EMBEDDED_REGEX, Pattern.CASE_INSENSITIVE)
  private val CLIENT_PATTERN = Pattern.compile(EMBEDDED_REGEX + "\\w*:?//",
    Pattern.CASE_INSENSITIVE)

  def init(): Unit = {
    // do nothing; just forces one-time invocation of various registerDialects
    SnappyStoreDialect.getClass
    SnappyStoreClientDialect.getClass
    SnappyDataPoolDialect.getClass
  }

  def canHandle(url: String): Boolean = {
    EMBEDDED_PATTERN.matcher(url).find() && !CLIENT_PATTERN.matcher(url).find()
  }

  override def addExtraDriverProperties(isLoner: Boolean,
      props: Properties): Unit = {
    if (!isLoner) {
      props.setProperty("host-data", "false")
      props.setProperty("queryHdfs", "")
    }
  }
}