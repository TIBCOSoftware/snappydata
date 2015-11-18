package io.snappydata

import java.util.Properties

import io.snappydata.impl.LeadImpl

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by soubhikc on 11/11/15.
  */
object ToolsCallbackImpl extends ToolsCallback {

  def invokeLeadStart(conf: SparkConf): Unit = {
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    lead.internalStart(conf)
  }

  override def invokeLeadStartAddonService(sc: SparkContext): Unit = {
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    lead.startAddOnServices(sc)
  }

  def invokeLeadStop(shutdownCredentials: Properties): Unit = {
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    lead.internalStop(shutdownCredentials)
  }

}
