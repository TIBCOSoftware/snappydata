/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql

import java.util.regex.Pattern

import com.pivotal.gemfirexd.Attribute
import io.snappydata.Constant
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.jdbc.JdbcDialects

/**
 * Default dialect for GemFireXD >= 1.4.0.
 * Contains specific type conversions to and from Spark SQL catalyst types.
 */
@DeveloperApi
case object SnappyStoreClientDialect extends SnappyDataBaseDialect {

  // register the dialect
  JdbcDialects.registerDialect(SnappyStoreClientDialect)

  private val CLIENT_PATTERN = Pattern.compile(
    s"^(${Constant.DEFAULT_THIN_CLIENT_URL}|${Attribute.DNC_PROTOCOL})", Pattern.CASE_INSENSITIVE)

  def canHandle(url: String): Boolean = CLIENT_PATTERN.matcher(url).find()
}
