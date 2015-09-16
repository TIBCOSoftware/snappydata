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
