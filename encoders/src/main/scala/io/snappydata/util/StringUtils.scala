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

import java.text.NumberFormat
import javax.annotation.Nonnull

import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

object StringUtils {

  val numFormatter: NumberFormat = java.text.NumberFormat.getInstance

  def cloneIfRequired(@Nonnull s: UTF8String): UTF8String = {
    if (s.getBaseOffset == Platform.BYTE_ARRAY_OFFSET &&
        s.getBaseObject.asInstanceOf[Array[Byte]].length == s.numBytes) {
      s
    } else {
      s.clone()
    }
  }

  /**
   * Defines a few String Interpolators.  Interpolators are things like
   * the built-in s"Hello $name", where the "s" interpolator plugs in the
   * value of the s variable.
   *
   * To use any of these interpolators, you must use this include:
   * import io.snappydata.util.StringUtils._
   *
   * TODO: Add some more, e.g., loginfo that prepends Thread name, pid,
   * etc.?
   */
  implicit class SnappyInterpolator(val sc: StringContext) extends AnyVal {

    /**
     * The pn string interpolator "pretty numbers" prints numbers using
     * the default number format (i.e., it automatically puts in commas
     * (thousands separator)).
     *
     * Usage:
     * val x = 1001; println( pn"$x" ) // prints "1,001"
     */
    def pn(args: Any*): String = {
      val strings = sc.parts.iterator
      val expressions = args.iterator
      val buf = new StringBuilder().append(strings.next())
      while (strings.hasNext) {
        val f: String = expressions.next() match {
          case n: java.lang.Number => numFormatter.format(n)
          case x => x.toString
        }
        buf append f
        buf append strings.next
      }
      buf.toString()
    }

    /**
     * Prepends a string with the name of the current thread, and
     * processes the rest of the string as if with s"..."
     */
    def ti(args: Any*): String = {
      s"[${Thread.currentThread().getName}] ${sc.s(args: _*)}"
    }
  }

}
