package org.apache.spark.streaming

import org.apache.log4j.Logger

class StopWatch(target: String, logger: Logger)  {

  private val _logger = logger
  private var _target = target
  private var _start = 0L

  def start() = {
      _logger info s"starting  ${_target}"
      _start = System.currentTimeMillis
  }

  def start(target: String): Unit = {
      _target = target
      start
  }

  def stop() = {
      val end = System.currentTimeMillis
      _logger info s"completed ${_target} in ${end - _start} msec"
  }

}
