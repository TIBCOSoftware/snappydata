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
package org.apache.spark

import java.io.File

object TestPackageUtils {

  private val userDir = System.getProperty("user.dir")

  def destDir: File = {
    val jarDir = new File(s"$userDir/jars")
    if (!jarDir.exists()) {
      jarDir.mkdir()
    }
    jarDir
  }

  def createJarFile(files: Seq[File], filePrefix: Option[String] = None): String = {
    val jarFile = new File(destDir, "testJar-%s.jar".format(System.currentTimeMillis()))
    TestUtils.createJar(files, jarFile, filePrefix)
    jarFile.getPath
  }
}
