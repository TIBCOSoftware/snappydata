/*
 *
 */
package org.apache.spark.sql.sources

import java.util.Properties

import org.apache.spark.sql.jdbc.JdbcDialect

// IMPORTANT: if any changes are made to this class then update the
// serialization correspondingly in ConnectionPropertiesSerializer
case class ConnectionProperties(url: String, driver: String,
    dialect: JdbcDialect, poolProps: Map[String, String],
    connProps: Properties, executorConnProps: Properties, hikariCP: Boolean)