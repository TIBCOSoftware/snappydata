package io.snappydata

import spark.jobserver.SparkJobBase

import org.apache.spark.sql.SnappyContext

/**
 * Created by soubhikc on 22/10/15.
 */
trait SnappySQLJob  extends SparkJobBase {
  type C = SnappyContext

}
