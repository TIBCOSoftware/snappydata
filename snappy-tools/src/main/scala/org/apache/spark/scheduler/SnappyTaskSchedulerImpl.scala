package org.apache.spark.scheduler

import org.apache.spark.SparkContext
import org.apache.spark.sql.SnappyContext

/**
  * Created by soubhikc on 16/11/15.
  */
private[spark] class SnappyTaskSchedulerImpl(sc: SparkContext) extends TaskSchedulerImpl(sc) {

  override def postStartHook(): Unit = {
    SnappyContext(sc)
    super.postStartHook()
  }

}
