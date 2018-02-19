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
package org.apache.spark.sql.sources

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable

/**
 * Tracks the child <code>DependentRelation</code>s for all
 * <code>ParentRelation</code>s. This is an optimization for faster access
 * to avoid scanning the entire catalog.
 */
object DependencyCatalog {

  private[this] val parentToDependentsMap =
    new mutable.HashMap[String, mutable.HashSet[String]]()

  private[this] val lock = new ReentrantReadWriteLock()

  def addDependents(tableName: String, dependents: Seq[String]): Unit = {
    val sync = lock.writeLock()
    sync.lock()
    try {
      parentToDependentsMap.get(tableName) match {
        case Some(dependencies) => dependencies ++= dependents
        case None =>
          val dependencies = new mutable.HashSet[String]()
          dependencies ++= dependents
          parentToDependentsMap += (tableName -> dependencies)
      }
    } finally {
      sync.unlock()
    }
  }

  def addDependent(tableName: String, dependent: String): Boolean = {
    val sync = lock.writeLock()
    sync.lock()
    try {
      parentToDependentsMap.get(tableName) match {
        case Some(dependencies) => dependencies.add(dependent)
        case None =>
          val dependencies = new mutable.HashSet[String]()
          parentToDependentsMap += (tableName -> dependencies)
          dependencies.add(dependent)
      }
    } finally {
      sync.unlock()
    }
  }

  def removeDependent(tableName: String, dependent: String): Boolean = {
    val sync = lock.writeLock()
    sync.lock()
    try {
      parentToDependentsMap.get(tableName).map(_.remove(dependent))
          .exists(identity)
    } finally {
      sync.unlock()
    }
  }

  def removeAllDependents(tableName: String): Boolean = {
    val sync = lock.writeLock()
    sync.lock()
    try {
      parentToDependentsMap.remove(tableName).isDefined
    } finally {
      sync.unlock()
    }
  }

  def getDependents(tableName: String): Seq[String] = {
    val sync = lock.readLock()
    sync.lock()
    try {
      parentToDependentsMap.get(tableName).map(_.toSeq).getOrElse(Nil)
    } finally {
      sync.unlock()
    }
  }

  def clear(): Unit = {
    val sync = lock.writeLock()
    sync.lock()
    try {
      parentToDependentsMap.clear()
    } finally {
      sync.unlock()
    }
  }
}
