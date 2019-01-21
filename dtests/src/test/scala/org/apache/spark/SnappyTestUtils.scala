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

import java.io.{PrintWriter, File}
import java.net.URL
import javax.tools.JavaFileObject

import org.apache.spark.TestUtils.JavaSourceFromString
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.collection.{Utils => Utility}


object SnappyTestUtils extends Logging {

  private val SOURCE = JavaFileObject.Kind.SOURCE

  def verifyClassOnExecutors(snc: SnappyContext, className: String,
                             version: String, count: Int, pw: PrintWriter): Unit = {
    val countInstances = Utility.mapExecutors[Int](snc.sparkContext,
      () => {
        if (SnappyTestUtils.loadClass(className, version)) {
          Seq(1).iterator
        } else Iterator.empty
      }).length

    assert(countInstances == count)
    // scalastyle:off println
    pw.println("Class is available on all executors : numExecutors (" + countInstances + ") " +
        "having  the class " + className + " loaded, is same as numServers (" + count + ") in " +
        "test" + " and the class version is: " + version);
  }

  def getJavaSourceFromString(name: String, code: String): JavaSourceFromString = {
    new JavaSourceFromString(name, code)
  }

  def createCompiledClass(className: String,
                          destDir: File,
                          sourceFile: JavaSourceFromString,
                          classpathUrls: Seq[URL] = Seq()): File = {

    TestUtils.createCompiledClass(className, destDir, sourceFile, classpathUrls)
  }

  def createJarFile(files: Seq[File], tempDir: String): String = {
    val jarFile = new File(tempDir, "testJar-%s.jar".format(System.currentTimeMillis()))
    TestUtils.createJar(files, jarFile)
    jarFile.getName
  }

  @throws[ClassNotFoundException]
  def loadClass(className: String,
                version: String = ""): Boolean = {
    val catchExpectedException: Boolean = version.isEmpty
    val loader = Thread.currentThread().getContextClassLoader
    log.info("loader : " + loader)
    assert(loader != null)
    try {
      val fakeClass = loader.loadClass(className).newInstance()
      log.info("fakeClass : " + fakeClass)
      assert(fakeClass != null)
      log.info("fakeClass loading successful.. : " + fakeClass)
      assert(fakeClass.toString.equals(version))
      log.info("fakeClass version is as expected.. : " + version)
      true
    } catch {
      case cnfe: ClassNotFoundException =>
        if (!catchExpectedException) {
          log.info("fakeClass loading unsuccessful..  " + className + version)
          throw cnfe
        }
        else {
          log.info("In else loop..  " + className + version)
          false
        }
    }
  }

}
