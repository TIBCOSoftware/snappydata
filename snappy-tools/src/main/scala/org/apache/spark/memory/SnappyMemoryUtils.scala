package org.apache.spark.memory

import com.pivotal.gemfirexd.internal.engine.store.GemFireStore

/**
 * Created by shirishd on 29/10/15.
 */
object SnappyMemoryUtils {
  /**
   * Checks whether GemFire critical threshold is breached
   * @return
   */
  def isCriticalUp: Boolean = {
    Option(GemFireStore.getBootingInstance).exists(g => g.thresholdListener.isCritical)
  }

  /**
   * Checks whether GemFire eviction threshold is breached
   * @return
   */
  def isEvictionUp: Boolean = {
    Option(GemFireStore.getBootingInstance).exists(g => g.thresholdListener.isEviction)
  }
}
