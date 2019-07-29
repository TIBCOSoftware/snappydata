/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import org.apache.spark.TestUtils.JavaSourceFromString

object SparkUtilsAccess {

  private val userDir = System.getProperty("user.dir")

  def destDir: File = {
    val jarDir = new File(s"$userDir/jars")
    if (!jarDir.exists()) {
      jarDir.mkdir()
    }
    jarDir
  }

  def getJavaSourceFromString(name: String, code: String): JavaSourceFromString = {
    new JavaSourceFromString(name, code)
  }

  def createUDFClass(name: String, code: String): File = {
    TestUtils.createCompiledClass(name, destDir,
      getJavaSourceFromString(name, code), Nil)
  }

  def createJarFile(files: Seq[File]): String = {
    createJarFile(files, "testJar-%s.jar".format(System.currentTimeMillis()))
  }

  def createJarFile(files: Seq[File], jarName: String, recreate: Boolean = false): String = {
    var jarFile = new File(destDir, s"$jarName.jar")
    if (jarFile.exists() && recreate) {
      jarFile.delete()
      jarFile = new File(destDir, s"$jarName.jar")
    }
    TestUtils.createJar(files, jarFile)
    jarFile.getPath
  }

}
