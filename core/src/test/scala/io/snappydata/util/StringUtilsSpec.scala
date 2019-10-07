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

import io.snappydata.util.StringUtils._ // bring in implicits

/**
 * Spec for StringUtils
 */
class StringUtilsSpec extends FlatSpec with Matchers {
  "pn StringInterpolator" should "pretty print numbers" in {
    val d = 1000.01D
    pn"$d" should be ("1,000.01")

    val f = 1234.4321F
    pn"$f" should be ("1,234.432") // Default number format truncates to 3 dec points

    val l = 10000000000L
    pn"$l" should be ("10,000,000,000")

    val i = 12345
    pn"$i" should be ("12,345")

    val s = 12
    pn"$s" should be ("12")
  }

  "pn StringInterpolator" should "pass through other types" in {
    case class Fred(age: Short)

    val d = 1001
    val s = "dalmations"
    val b = true
    val f = Fred(23)
    pn"$d $s $b $f" should be ("1,001 dalmations true Fred(23)")
  }

  "ti StringInterpolator" should "print thread info" in {
    val x = "a string"
    ti"$x" should endWith ("a string")
    ti"$x" should startWith ("[")
  }
}
