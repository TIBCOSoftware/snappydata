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
package org.apache.spark.executor

import java.io.File
import java.net.URL

import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.SparkEnv
import org.apache.spark.util.{ChildFirstURLClassLoader, MutableURLClassLoader, Utils}

class SnappyExecutor(
    executorId: String,
    executorHostname: String,
    env: SparkEnv,
    userClassPath: Seq[URL] = Nil,
    isLocal: Boolean = false)
    extends Executor(executorId, executorHostname, env, userClassPath, isLocal) {

  override def createClassLoader(): MutableURLClassLoader = {
    // Bootstrap the list of jars with the user class path.
    val now = System.currentTimeMillis()
    userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }

    val currentLoader = Utils.getContextOrSparkClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }
    if (env.conf.getBoolean("spark.executor.userClassPathFirst", false)) {
      new SnappyChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new SnappyMutableURLClassLoader(urls, currentLoader)
    }
  }

}

class SnappyChildFirstURLClassLoader(urls: Array[URL], parent: ClassLoader)
    extends ChildFirstURLClassLoader(urls, parent) {

  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    try {
      Misc.getMemStore.getDatabase.getClassFactory.loadClassFromDB(name)
    } catch {
      case e: ClassNotFoundException =>
        super.loadClass(name, resolve)
    }
  }
}


class SnappyMutableURLClassLoader(urls: Array[URL], parent: ClassLoader)
    extends MutableURLClassLoader(urls, parent) {
  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    try {
      super.loadClass(name, resolve)
    } catch {
      case e: ClassNotFoundException =>
        Misc.getMemStore.getDatabase.getClassFactory.loadClassFromDB(name)
    }
  }
}