package org.apache.spark.sql.streaming

import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by ymahajan on 25/09/15.
 */
trait StreamPlan {
  def streamingSnappyCtx = StreamPlan.currentContext.get()

  def stream: DStream[Row]
}

object StreamPlan {
  val currentContext = new ThreadLocal[StreamingSnappyContext]()
}

