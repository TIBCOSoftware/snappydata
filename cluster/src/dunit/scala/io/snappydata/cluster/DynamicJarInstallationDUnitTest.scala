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


package io.snappydata.cluster

import org.apache.spark.TestUtils
import org.apache.spark.sql.SnappyContext
import org.apache.spark.util.SnappyUtils

class DynamicJarInstallationDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) {
  val firstJar = TestUtils.createJarWithClasses(
    classNames = Seq("FakeClass1", "FakeClass2", "FakeClass3"),
    toStringValue = "1")
  val secondJar = TestUtils.createJarWithClasses(
    classNames = Seq("FakeClass2", "FakeClass3", "FakeClass4"),
    toStringValue = "2")


  def testJarDeployedWithSparkContext(): Unit = {
    println("Namrata - test started")
    val loader = SnappyUtils.getSnappyStoreContextLoader(getClass.getClassLoader)
    Thread.currentThread().setContextClassLoader(loader)

    println("Namrata - before adding the jar")

    sc.addJar(firstJar.getPath)

    println("Namrata - after adding the jar")
    val snc = SnappyContext(sc)
    snc.sql("create table test (x int) using column")
    println("Now everything is set up ")

    val countInstances = org.apache.spark.sql.collection.Utils.mapExecutors(snc,
      () => {
        try {
          println(" Namrata - running on executors")
          val fakeClass =
            Thread.currentThread().getContextClassLoader.loadClass("FakeClass1").newInstance()
          assert(fakeClass.toString.equals("1"))
          println(" I am able to load the FakeClass1 using " +
              Thread.currentThread().getContextClassLoader +
              " where as class loadr is " + getClass.getClassLoader)

          Seq(1).iterator
        } catch {
          case cnfe: ClassNotFoundException => {
            // fail("UnExpected an ClassNotFoundException thrown")
            Iterator.empty
          }
        }
      }).count

    println(" Namrata total instances are " + countInstances)
    /*
        assert(countInstances == 1)

        // check jar is reloaded

        val oldFile = new File(firstJar.getPath)
        val newFile = new File(secondJar.getPath)
        if (oldFile.exists()) {
          oldFile.delete()
        }

        newFile.renameTo(oldFile)

        //  upoading a new jar file so that old is removed
        sc.addJar(newFile.getPath)

        countInstances = org.apache.spark.sql.collection.Utils.mapExecutors[Int](sc,
          (context: TaskContext, part: ExecutorLocalPartition) => {
            try {
              Thread.currentThread().getContextClassLoader.loadClass("FakeClass1").newInstance()
            //  fail("Expected an ClassNotFoundException to be thrown")
            } catch {
              case cnfe: ClassNotFoundException => {
                val fakeClass2 =
                  Thread.currentThread().getContextClassLoader.loadClass("FakeClass2").newInstance()
                assert(fakeClass2.toString.equals("2"))
              }
            }
            Seq(1).toIterator
          }).count*/


  }

}


/*
  class SqlTestJob extends SparkSqlJob {
    def validate(sql: SQLContext, config: Config): SparkJobValidation = SparkJobValid

    def runJob(sql: SQLContext, config: Config): Any = {
      org.apache.spark.sql.collection.Utils.mapExecutors[Int](sql, () => {
        try {
          Thread.currentThread().getContextClassLoader.loadClass("").newInstance()
          Seq(1).iterator
        } catch {
          case cnfe: ClassNotFoundException => Iterator.empty
        }
      }).count
    }
  }*/
