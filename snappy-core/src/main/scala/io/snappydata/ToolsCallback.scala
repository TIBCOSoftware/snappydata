package io.snappydata


import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by soubhikc on 11/11/15.
  */
trait ToolsCallback {

  def invokeLeadStartAddonService(sc: SparkContext)
  def invokeStartFabricServer(sc: SparkContext, hostData: Boolean)
  def invokeStopFabricServer(sc: SparkContext)
  def getLocatorJDBCURL(sc:SparkContext):String
}
