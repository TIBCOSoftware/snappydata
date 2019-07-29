/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark.util

import java.io.File
import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConverters._
import scala.io.Source

import com.gemstone.gemfire.internal.shared.ClientSharedUtils
import org.apache.commons.io.FileUtils

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

/**
 * Contains utility methods for cleaning of spark local directories left orphan due to scenario
 * like abrupt failure of JVM.
 */
object LocalDirectoryCleanupUtil extends Logging {

  private lazy val listFile = ".tempfiles.list"

  /**
   * Save block manager directories to metadata file `.tempfiles.list` which will be used to cleanup
   * these directories if left orphan due to abrupt JVM failure.
   */
  def save(): Unit = synchronized {
    if (SparkEnv.get != null) {
      val localDirs = SparkEnv.get.blockManager.diskBlockManager.localDirs.toList.asJava
      FileUtils.writeLines(new File(listFile), "UTF-8", localDirs, true)
    }
  }

  /**
   * Attempts to recursively delete all files/directories present in temp files list.
   * Also cleans the temp files list once deletion is complete.
   */
  def clean(): Unit = synchronized {
    val listFilePath = Paths.get(listFile)
    if (Files.exists(listFilePath)) {
      val fileSource = Source.fromFile(listFile, "UTF-8")
      try {
        fileSource.getLines().map(Paths.get(_)).foreach(delete)
      } finally {
        fileSource.close()
      }
      try {
        Files.delete(listFilePath)
      } catch {
        case ex: Exception => logError(s"Failure while deleting file: $listFile.", ex)
          System.exit(1)
      }
    }
  }

  private def delete(path: Path): Unit = {
    if (Files.exists(path)) {
      ClientSharedUtils.deletePath(path, false, false)
    } else {
      logInfo(s"File or directory does not exists : $path")
    }
  }
}