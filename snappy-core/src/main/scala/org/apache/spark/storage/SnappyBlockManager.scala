package org.apache.spark.storage

import org.apache.spark.network.{BlockDataManager, BlockTransferService}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.{SecurityManager, MapOutputTracker, SparkConf}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage._

/**
 * Created by shirishd on 12/10/15.
 */

private[spark] class SnappyBlockManager(
    executorId: String,
    rpcEnv: RpcEnv,
    override val master: BlockManagerMaster,
    defaultSerializer: Serializer,
    override val conf: SparkConf,
    mapOutputTracker: MapOutputTracker,
    shuffleManager: ShuffleManager,
    blockTransferService: BlockTransferService,
    securityManager: SecurityManager,
    numUsableCores: Int)
    extends BlockManager(executorId, rpcEnv, master, defaultSerializer, conf, mapOutputTracker,
      shuffleManager, blockTransferService, securityManager, numUsableCores) {


  override protected[spark] val memoryStore = Class.forName("org.apache.spark.storage.SnappyMemoryStore").
        //getConstructors()(0).newInstance(this, maxMemory: java.lang.Long).
        getConstructors()(0).newInstance(this, BlockManager.getMaxMemory(conf): java.lang.Long).
      asInstanceOf[MemoryStore]
}

object SnappyBlockManager {

}