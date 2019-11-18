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
package org.apache.spark.sql.test

import java.io.File

import io.snappydata.test.dunit.DistributedTestBase.InitializeRun
import org.scalatest.Tag

import org.apache.spark.SparkFunSuite

trait SnappySparkTestUtil extends SparkFunSuite {

  InitializeRun.setUp()

  def withDir(dirName: String)(f: => Unit): Unit = {
    new File(dirName).mkdir()
  }

  def excluded: Seq[String] = Nil

  def ignored: Seq[String] = Nil

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any /* Assertion */)
      (implicit pos: org.scalactic.source.Position): Unit = {
    if (!excluded.contains(testName)) {
      if (ignored.contains(testName)) {
        super.ignore(testName, testTags: _*)(testFun)
      } else {
        super.test(testName, testTags: _*)(testFun)
      }
    }
  }
}
