package io.snappydata.util

import java.text.SimpleDateFormat
import java.util.Date
import java.io.File
import java.lang.management.ManagementFactory

/**
  *  This can be mixed into a class (but not an object?) to get the right method
  */
trait DebugUtils {
  def msg(m: String) = DebugUtils.msg(m)
}


/**
  * Debuggin Utilities.
  *
  * To get the di"..." string interpolator to work, you'll need to add this
  * import:
  *  import io.snappydata.util.DebugUtils._
  */
object DebugUtils {
  val format = new SimpleDateFormat("mm:ss:SSS")

  // Apparently, its pretty hard to get the PID of the current process in Java?!
  // Anyway, here is one method that depends on /proc, but I think we are all
  // running on platforms that have /proc.  If not, we'll have to redo this on to
  // use the Java ManagementFactory.getRuntimemMXBean() method?  See
  // http://stackoverflow.com/questions/35842/how-can-a-java-program-get-its-own-process-id
  //
  // This should probably be in a OS-specific class?
  //lazy val myPid: Int = Integer.parseInt(new File("/proc/self").getCanonicalFile().getName())

  lazy val myInfo: String = ManagementFactory.getRuntimeMXBean().getName()

  /**
    * Print a message on stdout but prefix with thread info and timestamp info
    */
  def msg(m: String): Unit = println(di"m")

  /**
    * Get the PID for this JVM
    */
  def getPidInfo(): String = myInfo

  implicit class DebugInterpolator(val sc: StringContext) extends AnyVal {
    def di(args: Any*): String = {
      val ts = new Date(System.currentTimeMillis())
      s"==== [($myInfo) ${Thread.currentThread().getName()}: (${format.format(ts)})]:  ${sc.s(args:_*)}"
    }
  }
}
