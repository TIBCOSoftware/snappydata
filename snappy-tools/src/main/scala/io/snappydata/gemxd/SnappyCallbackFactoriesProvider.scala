package io.snappydata.gemxd

import com.pivotal.gemfirexd.internal.snappy.ClusterCallbacks

/**
 * This should hold the callback factories that are needed by the GemXD code to interact
 * with Snappy.
 *
 * Created by hemantb on 10/12/15.
 */
object SnappyCallbackFactoriesProvider {

  /**
   * Cluster related callbacl factory
   * @return
   */
  def getClusterCallbacksImpl():  ClusterCallbacks = {
    ClusterCallbacksImpl
  }

}
