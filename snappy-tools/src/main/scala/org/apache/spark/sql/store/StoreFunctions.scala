package org.apache.spark.sql.store

import java.sql.Connection

import scala.reflect.ClassTag

/**
 * Created by rishim on 3/11/15.
 */
object StoreFunctions {

  implicit def executeWithConnection[T: ClassTag](getConnection: () => Connection, f: PartialFunction[(Connection), T], closeOnSuccess: Boolean = true): T = {
    val conn = getConnection()
    var isClosed = false;
    try {
      f(conn)
    } catch {
      case t: Throwable => {
        conn.close()
        isClosed = true
        throw t;
      }
    } finally {
      if (closeOnSuccess && !isClosed) {
        conn.close()
      }
    }
  }

}
