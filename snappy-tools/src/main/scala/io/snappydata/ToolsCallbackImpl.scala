package io.snappydata

import io.snappydata.impl.LeadImpl

import org.apache.spark.SparkConf
import org.apache.spark.sql.SnappyContext

/**
  * Created by soubhikc on 11/11/15.
  */
object ToolsCallbackImpl extends ToolsCallback {

  def invokeLeadStart(conf: SparkConf): Unit = {
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    lead.internalStart(conf)
  }

  override def invokeLeadStartAddonService(snc: SnappyContext): Unit = {
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    // TODO: [soubhik] This needs to be in some other place.
    lead.startAddOnServices(snc)
  }
}
