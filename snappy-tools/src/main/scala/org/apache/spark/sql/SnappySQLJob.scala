package org.apache.spark.sql

import spark.jobserver.SparkJobBase

/**
 * Created by soubhikc on 22/10/15.
 */
trait SnappySQLJob  extends SparkJobBase {
  type C = SnappyContext

}
