package io.snappydata

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SnappyContext

/**
  * Created by soubhikc on 11/11/15.
  */
trait ToolsCallback {

  def invokeLeadStart(conf: SparkConf)

  def invokeLeadStartAddonService(snc: SnappyContext)

  def invokeLeadStop(shutdownCredentials: Properties)
}
