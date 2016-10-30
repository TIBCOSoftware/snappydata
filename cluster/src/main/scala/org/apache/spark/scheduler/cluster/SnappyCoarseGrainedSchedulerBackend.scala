/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.scheduler.cluster

import scala.collection.mutable

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.cache.CacheClosedException
import com.gemstone.gemfire.distributed.internal.MembershipListener
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils

import org.apache.spark.SparkContext
import org.apache.spark.rpc.{RpcEndpointAddress, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.LaunchTask
import org.apache.spark.sql.{BlockAndExecutorId, SnappyContext}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OpenHashMap

class SnappyCoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl,
    override val rpcEnv: RpcEnv)
    extends CoarseGrainedSchedulerBackend(scheduler, rpcEnv) {

  private val snappyAppId = "snappy-app-" + System.currentTimeMillis

  val membershipListener = new MembershipListener {
    override def quorumLost(failures: java.util.Set[InternalDistributedMember],
        remaining: java.util.List[InternalDistributedMember]): Unit = {}

    override def memberJoined(id: InternalDistributedMember): Unit = {}

    override def memberSuspect(id: InternalDistributedMember,
        whoSuspected: InternalDistributedMember): Unit = {}

    override def memberDeparted(id: InternalDistributedMember, crashed: Boolean): Unit = {
      SnappyContext.removeBlockId(id.toString)
    }
  }

  /**
   * Overriding the spark app id function to provide a snappy specific app id.
   *
   * @return An application ID
   */
  override def applicationId(): String = snappyAppId

  @volatile private var _driverUrl: String = ""

  def driverUrl: String = _driverUrl

  override def start() {

    super.start()
    _driverUrl = RpcEndpointAddress(
      scheduler.sc.conf.get("spark.driver.host"),
      scheduler.sc.conf.get("spark.driver.port").toInt,
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
    GemFireXDUtils.getGfxdAdvisor.getDistributionManager
        .addMembershipListener(membershipListener)
    logInfo(s"started with driverUrl $driverUrl")
  }

  override def stop() {
    super.stop()
    _driverUrl = ""
    SnappyClusterManager.cm.foreach(_.stopLead())
    try {
      GemFireXDUtils.getGfxdAdvisor.getDistributionManager
          .removeMembershipListener(membershipListener)
    } catch {
      case cce: CacheClosedException =>
    }
    logInfo(s"stopped successfully")
  }

  override protected def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    // keep the app id as part of driver property so that it can be retrieved
    // by the executor when driver properties are fetched using
    // [org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RetrieveSparkProps]
    new SnappyDriverEndpoint(rpcEnv, properties ++
        Seq[(String, String)](("spark.app.id", applicationId())))
  }

  class SnappyDriverEndpoint(override val rpcEnv: RpcEnv,
      sparkProperties: Seq[(String, String)])
      extends DriverEndpoint(rpcEnv, sparkProperties) {

    override def launchTasks(tasks: Seq[Seq[TaskDescription]]): Unit = {
      val executorTaskGroupMap = new OpenHashMap[String, ExecutorTaskGroup](8)
      for (taskSet <- tasks) {
        for (task <- taskSet) {
          val taskLimit = task.serializedTask.limit
          val taskSize = taskLimit + task.taskData.length
          if (checkTaskSizeLimit(task, taskSize)) {
            // group tasks per executor as long as message limit is not breached
            executorTaskGroupMap.changeValue(task.executorId, {
              val executorData = executorDataMap(task.executorId)
              val executorTaskGroup = new ExecutorTaskGroup(executorData, taskSize)
              executorTaskGroup.taskGroup += task
              executorTaskGroup.taskDataList += task.taskData
              task.taskData = Task.EMPTY
              task.taskDataReference = 0
              executorTaskGroup
            }, { executorTaskGroup =>
              // group into existing if size fits in the max allowed
              if (!executorTaskGroup.addTask(task, taskLimit, maxRpcMessageSize)) {
                // send this task separately
                val executorData = executorTaskGroup.executorData
                executorData.freeCores -= scheduler.CPUS_PER_TASK
                logInfo(s"Launching task ${task.taskId} on executor id: " +
                    s"${task.executorId} hostname: ${executorData.executorHost}.")

                executorData.executorEndpoint.send(LaunchTask(task))
              }
              executorTaskGroup
            })
          }
        }
      }
      // send the accumulated task groups per executor
      executorTaskGroupMap.foreach { case (executorId, executorTaskGroup) =>
        val taskGroup = executorTaskGroup.taskGroup
        val executorData = executorTaskGroup.executorData

        executorData.freeCores -= (scheduler.CPUS_PER_TASK * taskGroup.length)
        logDebug(s"Launching tasks ${taskGroup.map(_.taskId).mkString(",")} on " +
            s"executor id: $executorId hostname: ${executorData.executorHost}.")
        executorData.executorEndpoint.send(LaunchTasks(taskGroup,
          executorTaskGroup.taskDataList))
      }
    }
  }

}

final class ExecutorTaskGroup(private[cluster] var executorData: ExecutorData,
    private var groupSize: Int = 0) {
  private[cluster] val taskGroup = new mutable.ArrayBuffer[TaskDescription](2)
  // field to carry around common task data
  private[cluster] val taskDataList = new mutable.ArrayBuffer[Array[Byte]](2)

  def addTask(task: TaskDescription, taskLimit: Int, limit: Int): Boolean = {
    val newGroupSize = groupSize + taskLimit
    if (newGroupSize > limit) return false

    groupSize = newGroupSize
    // linear search is best since there cannot be many different
    // tasks in a single taskSet
    val taskDataLen = task.taskData.length
    if (taskDataLen == 0 || findOrAddTaskData(task, taskDataList, limit)) {
      taskGroup += task
      true
    } else {
      // task rejected from group
      groupSize -= taskLimit
      false
    }
  }

  private def findOrAddTaskData(task: TaskDescription,
      taskDataList: mutable.ArrayBuffer[Array[Byte]], limit: Int): Boolean = {
    val data = task.taskData
    val numData = taskDataList.length
    var i = 0
    while (i < numData) {
      if (taskDataList(i) eq data) {
        task.taskData = Task.EMPTY
        task.taskDataReference = i
        return true
      }
      i += 1
    }
    val newGroupSize = groupSize + data.length
    if (newGroupSize <= limit) {
      groupSize = newGroupSize
      taskDataList += data
      task.taskData = Task.EMPTY
      task.taskDataReference = numData
      true
    } else false
  }
}

case class LaunchTasks(private var tasks: mutable.ArrayBuffer[TaskDescription],
    private var taskDataList: mutable.ArrayBuffer[Array[Byte]])
    extends Serializable with KryoSerializable {

  override def write(kryo: Kryo, output: Output): Unit = Utils.tryOrIOException {
    val tasks = this.tasks
    val numTasks = tasks.length
    output.writeVarInt(numTasks, true)
    var i = 0
    while (i < numTasks) {
      tasks(i).write(kryo, output)
      i += 1
    }
    val taskDataList = this.taskDataList
    val numData = taskDataList.length
    output.writeVarInt(numData, true)
    i = 0
    while (i < numData) {
      val data = taskDataList(i)
      output.writeInt(data.length)
      output.writeBytes(data, 0, data.length)
      i += 1
    }
  }

  override def read(kryo: Kryo, input: Input): Unit = Utils.tryOrIOException {
    var numTasks = input.readVarInt(true)
    val tasks = new mutable.ArrayBuffer[TaskDescription](numTasks)
    while (numTasks > 0) {
      val task = new TaskDescription(0, 0, null, null, 0, null)
      task.read(kryo, input)
      tasks += task
      numTasks -= 1
    }
    var numData = input.readVarInt(true)
    val taskDataList = new mutable.ArrayBuffer[Array[Byte]](numData)
    while (numData > 0) {
      val len = input.readInt()
      taskDataList += input.readBytes(len)
      numData -= 1
    }
    this.tasks = tasks
    this.taskDataList = taskDataList
  }
}

class BlockManagerIdListener(sc: SparkContext)
    extends SparkListener {

  override def onExecutorAdded(
      msg: SparkListenerExecutorAdded): Unit = synchronized {
    val executorCores = msg.executorInfo.totalCores
    val profile = Misc.getMemStore.getDistributionAdvisor
        .getProfile(msg.executorId)
    val numProcessors = if (profile != null) profile.getNumProcessors
    else executorCores
    SnappyContext.getBlockId(msg.executorId) match {
      case None => SnappyContext.addBlockId(msg.executorId,
        new BlockAndExecutorId(null, executorCores, numProcessors))
      case Some(b) => b._executorCores = executorCores
        b._numProcessors = numProcessors
    }
  }

  override def onBlockManagerAdded(
      msg: SparkListenerBlockManagerAdded): Unit = synchronized {
    val executorId = msg.blockManagerId.executorId
    SnappyContext.getBlockIdIfNull(executorId) match {
      case None => SnappyContext.addBlockId(executorId,
        new BlockAndExecutorId(msg.blockManagerId,
          sc.schedulerBackend.defaultParallelism(),
          Runtime.getRuntime.availableProcessors()))
      case Some(b) => b._blockId = msg.blockManagerId
    }
  }

  override def onBlockManagerRemoved(
      msg: SparkListenerBlockManagerRemoved): Unit = {
    SnappyContext.removeBlockId(msg.blockManagerId.executorId)
  }

  override def onExecutorRemoved(msg: SparkListenerExecutorRemoved): Unit =
    SnappyContext.removeBlockId(msg.executorId)

  override def onApplicationEnd(msg: SparkListenerApplicationEnd): Unit =
    SnappyContext.clearBlockIds()
}
