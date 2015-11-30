package org.apache.spark.sql.streaming

import org.apache.spark.util.Utils

/**
 * Created by ymahajan on 25/09/15.
 */
object StreamUtils {

  def invoke(
              clazz: Class[_],
              obj: AnyRef,
              methodName: String,
              args: (Class[_], AnyRef)*): AnyRef = {
    val (types, values) = args.unzip
    val method = clazz.getDeclaredMethod(methodName, types: _*)
    method.setAccessible(true)
    method.invoke(obj, values.toSeq: _*)
  }

  def loadClass(className: String): Class[_] = {
    try {
      Utils.getContextOrSparkClassLoader.loadClass(className)
    } catch {
      case cnf: java.lang.ClassNotFoundException =>
        sys.error(s"Failed to load class : $className")
    }
  }

}