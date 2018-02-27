/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.scheduler

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.{LongFunction, ToLongFunction}

import scala.collection.mutable.ArrayBuffer

import com.koloboke.function.{LongObjPredicate, ObjLongToLongFunction}
import io.snappydata.Property
import io.snappydata.collection.{LongObjectHashMap, ObjectLongHashMap}

import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.sql.{BlockAndExecutorId, SnappyContext}
import org.apache.spark.{SparkContext, SparkException, TaskNotSerializableException}

private[spark] class SnappyTaskSchedulerImpl(sc: SparkContext) extends TaskSchedulerImpl(sc) {

  override def postStartHook(): Unit = {
    SnappyContext.initGlobalSparkContext(sc)
    super.postStartHook()
  }

  private type CoresAndAttempts = (ObjectLongHashMap[String], LongObjectHashMap[TaskSetManager])

  private val limitJobCores = Property.LimitJobCores.get(sc.conf)

  private val (maxExecutorTaskCores, numExecutors) = {
    val map = new ConcurrentHashMap[String, Integer](16, 0.7f, 1)
    if (limitJobCores) {
      for ((executorId, blockId) <- SnappyContext.getAllBlockIds) {
        addBlockId(executorId, blockId, map)
      }
    }
    (map, new AtomicInteger(map.size()))
  }
  private val stageCoresAndAttempts =
    LongObjectHashMap.withExpectedSize[CoresAndAttempts](32)
  private val taskIdExecutorAndManager =
    LongObjectHashMap.withExpectedSize[(String, TaskSetManager)](32)

  private val createNewStageMap = new LongFunction[CoresAndAttempts] {
    override def apply(stageId: Long): CoresAndAttempts =
      ObjectLongHashMap.withExpectedSize[String](8) ->
          LongObjectHashMap.withExpectedSize[TaskSetManager](2)
  }
  private val lookupExecutorCores = new ToLongFunction[String] {
    override def applyAsLong(executorId: String): Long = {
      maxExecutorTaskCores.get(executorId) match {
        case null => Int.MaxValue // no restriction
        case c => c.intValue()
      }
    }
  }
  private val addCPUsOnTaskFinish = new ObjLongToLongFunction[String] {
    override def applyAsLong(execId: String, availableCores: Long): Long =
      availableCores + CPUS_PER_TASK
  }

  private def addBlockId(executorId: String, blockId: BlockAndExecutorId,
      map: ConcurrentHashMap[String, Integer]): Boolean = {
    if (limitJobCores && blockId.executorCores > 0 && blockId.numProcessors > 0 &&
        blockId.numProcessors < blockId.executorCores) {
      map.put(executorId, Int.box(blockId.numProcessors)) == null
    } else false
  }

  private[spark] def addBlockId(executorId: String, blockId: BlockAndExecutorId): Unit = {
      if (addBlockId(executorId, blockId, maxExecutorTaskCores)) {
      numExecutors.incrementAndGet()
    }
  }

  private[spark] def removeBlockId(executorId: String): Unit = {
    maxExecutorTaskCores.remove(executorId) match {
      case null =>
      case _ => numExecutors.decrementAndGet()
    }
  }

  override protected def getTaskSetManagerForSubmit(taskSet: TaskSet): TaskSetManager = {
    val manager = createTaskSetManager(taskSet, maxTaskFailures)
    val stage = taskSet.stageId
    val (stageAvailableCores, stageTaskSets) = stageCoresAndAttempts.computeIfAbsent(
      stage, createNewStageMap)
    val conflictingTaskSet = !stageTaskSets.forEachWhile(new LongObjPredicate[TaskSetManager] {
      override def test(attempt: Long, ts: TaskSetManager): Boolean = {
        ts.taskSet == taskSet || ts.isZombie
      }
    })
    if (conflictingTaskSet) {
      throw new IllegalStateException(
        s"more than one active taskSet for stage $stage: $stageTaskSets")
    }
    if (stageAvailableCores.size() > 0) stageAvailableCores.clear()
    stageTaskSets.justPut(taskSet.stageAttemptId, manager)
    manager
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo(s"Cancelling stage $stageId")
    stageCoresAndAttempts.get(stageId) match {
      case null =>
      case (_, attempts) => attempts.forEachWhile(new LongObjPredicate[TaskSetManager] {
        override def test(attempt: Long, tsm: TaskSetManager): Boolean = {
          // There are two possible cases here:
          // 1. The task set manager has been created and some tasks have been scheduled.
          //    In this case, send a kill signal to the executors to kill the task
          //    and then abort the stage.
          // 2. The task set manager has been created but no tasks has been scheduled.
          //    In this case, simply abort the stage.
          tsm.runningTasksSet.foreach { tid =>
            val execId = taskIdExecutorAndManager.get(tid)._1
            backend.killTask(tid, execId, interruptThread)
          }
          val msg = s"Stage $stageId cancelled"
          tsm.abort(msg)
          logInfo(msg)
          true
        }
      })
    }
  }

  override def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    val taskSet = manager.taskSet
    stageCoresAndAttempts.get(taskSet.stageId) match {
      case null =>
      case (_, taskSetsForStage) =>
        taskSetsForStage.remove(taskSet.stageAttemptId)
        if (taskSetsForStage.size() == 0) {
          stageCoresAndAttempts.remove(taskSet.stageId)
        }
    }
    manager.parent.removeSchedulable(manager)
    if (isInfoEnabled) {
      logInfo(s"Removed TaskSet ${taskSet.id}, whose tasks have all completed, from pool " +
          manager.parent.name)
    }
  }

  /**
   * Avoid giving all the available cores on a node to a single task. This serves two purposes:
   *
   * a) Keeps some cores free for any concurrent tasks that may be submitted after
   * the first has been scheduled.
   *
   * b) Since the snappy executors use (2 * physical cores) to aid in more concurrency,
   * it helps reduce disk activity for a single task and improves performance for
   * disk intensive queries.
   */
  override protected def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],
      availableCpus: Array[Int],
      tasks: IndexedSeq[ArrayBuffer[TaskDescription]]): Boolean = {
    // reduce the available CPUs for a single taskSet if more than physical cores are exposed
    val availableCores = if (numExecutors.get() > 0) {
      val coresAndAttempts = stageCoresAndAttempts.get(taskSet.taskSet.stageId)
      if (coresAndAttempts ne null) coresAndAttempts._1 else null
    } else null
    var launchedTask = false
    for (i <- shuffledOffers.indices) {
      val execId = shuffledOffers(i).executorId
      if ((availableCpus(i) >= CPUS_PER_TASK) &&
          ((availableCores eq null) ||
              (availableCores.computeIfAbsent(execId, lookupExecutorCores) >= CPUS_PER_TASK))) {
        try {
          val host = shuffledOffers(i).host
          for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
            tasks(i) += task
            val tid = task.taskId
            taskIdExecutorAndManager.justPut(tid, execId -> taskSet)
            executorIdToRunningTaskIds(execId).add(tid)
            if (availableCores ne null) {
              availableCores.addValue(execId, -CPUS_PER_TASK)
            }
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            launchedTask = true
          }
        } catch {
          case _: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    }
    launchedTask
  }

  override protected[scheduler] def getTaskSetManager(taskId: Long): Option[TaskSetManager] = {
    taskIdExecutorAndManager.get(taskId) match {
      case null => None
      case (_, manager) => Some(manager)
    }
  }

  override protected def getExecutorAndManager(
      taskId: Long): Option[(() => String, TaskSetManager)] = {
    taskIdExecutorAndManager.get(taskId) match {
      case null => None
      case (execId, manager) => Some(() => execId, manager)
    }
  }

  override def error(message: String): Unit = synchronized {
    if (stageCoresAndAttempts.size() > 0) {
      // Have each task set throw a SparkException with the error
      stageCoresAndAttempts.forEachWhile(new LongObjPredicate[CoresAndAttempts] {
        override def test(stageId: Long, p: CoresAndAttempts): Boolean = {
          p._2.forEachWhile(new LongObjPredicate[TaskSetManager] {
            override def test(attempt: Long, manager: TaskSetManager): Boolean = {
              try {
                manager.abort(message)
              } catch {
                case e: Exception => logError("Exception in error callback", e)
              }
              true
            }
          })
        }
      })
    }
    else {
      // No task sets are active but we still got an error. Just exit since this
      // must mean the error is during registration.
      // It might be good to do something smarter here in the future.
      throw new SparkException(s"Exiting due to error from cluster scheduler: $message")
    }
  }

  override protected def cleanupTaskState(tid: Long): Unit = {
    taskIdExecutorAndManager.remove(tid) match {
      case null =>
      case (executorId, taskSet) =>
        executorIdToRunningTaskIds.get(executorId) match {
          case Some(s) => s.remove(tid)
          case None =>
        }
        stageCoresAndAttempts.get(taskSet.taskSet.stageId) match {
          case null =>
          case (cores, _) => cores.computeIfPresent(executorId, addCPUsOnTaskFinish)
        }
    }
  }

  override private[scheduler] def taskSetManagerForAttempt(
      stageId: Int, stageAttemptId: Int): Option[TaskSetManager] = {
    stageCoresAndAttempts.get(stageId) match {
      case null => None
      case (_, attempts) => Option(attempts.get(stageAttemptId))
    }
  }
}
