/*
 *
 */
package org.apache.spark.sql.row

import java.util.regex.Pattern

import com.pivotal.gemfirexd.Attribute
import io.snappydata.Constant

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.SnappyDataBaseDialect
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