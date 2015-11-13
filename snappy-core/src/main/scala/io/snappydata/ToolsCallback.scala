package io.snappydata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SnappyContext

/**
  * Created by soubhikc on 11/11/15.
  */
trait ToolsCallback {

  def invokeLeadStart(conf: SparkConf)

  def invokeLeadStartAddonService(snc: SnappyContext)
}
