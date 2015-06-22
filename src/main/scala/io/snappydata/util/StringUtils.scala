package io.snappydata.util

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object StringUtils {
  val numFormatter = java.text.NumberFormat.getInstance

  /**
    * Defines a few String Interpolators.  Interpolators are things like
    * the built-in s"Hello $name", where the "s" interpolator plugs in the
    * value of the s variable.
    *
    *  To use any of these interpolators, you must use this include:
    *    import io.snappydata.util.StringUtils._
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
      var buf = new StringBuilder(strings.next)
      while(strings.hasNext) {
        val f: String = expressions.next match {
          case n: java.lang.Number => numFormatter.format(n)
          case x => x.toString
        }
        buf append f
        buf append strings.next
      }
      buf.toString
    }

    /**
      * Prepends a string with the name of the current thread, and
      * processes the rest of the string as if with s"..."
      */
    def ti(args: Any*): String = {
      s"[${Thread.currentThread().getName()}] ${sc.s(args:_*)}"
    }
  }
}
