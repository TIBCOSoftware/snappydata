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
package io.snappydata.util

import org.scalatest._
import Inspectors._  // picks up forAll () {}

import io.snappydata.util.DebugUtils._ // bring in implicits

/**
 * Spec for StringUtils
 */
class DebugUtilsSpec extends FlatSpec with Matchers {
  "di StringInterpolator" should "add debug info" in {
    val x = "a string"
    val i = 12

    // "==== [(62412@pspace.local) pool-23-thread-6-ScalaTest-running-DebugUtilsSpec: (55:35:834)]:  a string 12"
    di"$x $i" should endWith ("a string 12")
    di"$x $i" should startWith ("==== [")
  }
}
