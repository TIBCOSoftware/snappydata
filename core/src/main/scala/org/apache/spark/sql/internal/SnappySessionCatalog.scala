package org.apache.spark.sql.internal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalog, SessionCatalog, FunctionResourceLoader}
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.hive.client.HiveClient

private[sql] class SnappySessionCatalog(
    externalCatalog: ExternalCatalog,
    val sparkSession: SparkSession,
    functionResourceLoader: FunctionResourceLoader,
    functionRegistry: FunctionRegistry,
    val conf: SQLConf,
    hadoopConf: Configuration)
    extends SessionCatalog(
      externalCatalog,
      functionResourceLoader,
      functionRegistry,
      conf,
      hadoopConf) {

      }