/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import java.io.{OutputStream, PrintStream}
import java.util.concurrent.ThreadLocalRandom

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.util.Random

import org.apache.commons.io.output.TeeOutputStream

import org.apache.spark.util.Benchmark.Result

/**
 * Copy of BenchMark for specific purpose
 *
 * Utility class to benchmark components. An example of how to use this is:
 *  val benchmark = new Benchmark("My Benchmark", valuesPerIteration)
 *   benchmark.addCase("V1")(<function>)
 *   benchmark.addCase("V2")(<function>)
 *   benchmark.run
 * This will output the average time to run each function and the rate of each function.
 *
 * The benchmark function takes one argument that is the iteration that's being run.
 *
 * @param name name of this benchmark.
 * @param valuesPerIteration number of values used in the test case, used to compute rows/s.
 * @param minNumIters the min number of iterations that will be run per case, not counting warm-up.
 * @param warmupTime amount of time to spend running dummy case iterations for JIT warm-up.
 * @param minTime further iterations will be run for each case until this time is used up.
 * @param outputPerIteration if true, the timing for each run will be printed to stdout.
 * @param output optional output stream to write benchmark results to
 */
private[spark] class QueryBenchmark(
    name: String,
    valuesPerIteration: Long,
    minNumIters: Int = 2,
    warmupTime: FiniteDuration = 2.seconds,
    minTime: FiniteDuration = 2.seconds,
    outputPerIteration: Boolean = false,
    output: Option[OutputStream] = None) {
  import QueryBenchmark._
  val benchmarks = mutable.ArrayBuffer.empty[QueryBenchmark.Case]
  val out = if (output.isDefined) {
    new PrintStream(new TeeOutputStream(System.out, output.get))
  } else {
    System.out
  }

  /**
   * Adds a case to run when run() is called. The given function will be run for several
   * iterations to collect timing statistics.
   *
   * @param name of the benchmark case
   * @param numIters if non-zero, forces exactly this many iterations to be run
   */
  def addCase(
      name: String,
      numIters: Int = 0,
      prepare: () => Unit = () => { },
      cleanup: () => Unit = () => { })(f: Int => Boolean): Unit = {
    val timedF = (timer: Benchmark.Timer) => {
      timer.startTiming()
      val ret = f(timer.iteration)
      timer.stopTiming()
      ret
    }
    benchmarks += QueryBenchmark.Case(name, timedF, numIters, prepare, cleanup)
  }

  /**
   * Adds a case with manual timing control. When the function is run, timing does not start
   * until timer.startTiming() is called within the given function. The corresponding
   * timer.stopTiming() method must be called before the function returns.
   *
   * @param name of the benchmark case
   * @param numIters if non-zero, forces exactly this many iterations to be run
   */
  def addTimerCase(name: String, numIters: Int = 0)(f: Benchmark.Timer => Boolean): Unit = {
    benchmarks += QueryBenchmark.Case(name, f, numIters)
  }

  /**
   * Runs the benchmark and outputs the results to stdout. This should be copied and added as
   * a comment with the benchmark. Although the results vary from machine to machine, it should
   * provide some baseline.
   */
  def run(): Unit = {
    require(benchmarks.nonEmpty)
    // scalastyle:off
    println("Running benchmark: " + name)

    val results = benchmarks.map { c =>
      println("  Running case: " + c.name)
      try {
        c.prepare()
        measure(valuesPerIteration, c.numIters)(c.fn)
      } finally {
        c.cleanup()
      }
    }
    println

    val firstBest = results.head.bestMs
    // The results are going to be processor specific so it is useful to include that.
    out.println(Benchmark.getJVMOSInfo())
    out.println(Benchmark.getProcessorName())
    out.printf("%-40s %16s %12s %13s %10s\n", name + ":", "Best/Avg Time(ms)", "Rate(M/s)",
      "Per Row(ns)", "Relative")
    out.println("-" * 96)
    results.zip(benchmarks).foreach { case (result, benchmark) =>
      out.printf("%-40s %16s %12s %13s %10s\n",
        benchmark.name,
        "%5.0f / %4.0f" format (result.bestMs, result.avgMs),
        "%10.1f" format result.bestRate,
        "%6.1f" format (1000 / result.bestRate),
        "%3.1fX" format (firstBest / result.bestMs))
    }
    out.println
    // scalastyle:on
  }

  /**
   * Runs a single function `f` for iters, returning the average time the function took and
   * the rate of the function.
   */
  def measure(num: Long, overrideNumIters: Int)(f: Benchmark.Timer => Boolean): Result = {
    System.gc()  // ensures garbage from previous cases don't impact this one
    val warmupDeadline = warmupTime.fromNow
    while (!warmupDeadline.isOverdue) {
      f(new Benchmark.Timer(-1))
    }
    val minIters = if (overrideNumIters != 0) overrideNumIters else minNumIters
    val minDuration = if (overrideNumIters != 0) 0 else minTime.toNanos
    val runTimes = ArrayBuffer[Long]()
    var i = 0
    while (i < minIters || runTimes.sum < minDuration) {
      var j = 1
      while (j < 101) {
        val timer = new Benchmark.Timer(i)
        val ret = f(timer)
        val runTime = timer.totalTime()
        if (ret || j == 100) {
          runTimes += runTime
          if (outputPerIteration) {
            // scalastyle:off
            println(s"Iteration $i took ${runTime / 1000} microseconds")
            // scalastyle:on
          }
          if (j == 100) {
            setRandomValues(valuesPerIteration)
          }
          j = 101
        } else {
          setRandomValues(valuesPerIteration)
          if (outputPerIteration) {
            // scalastyle:off
            println(s"Iteration $i attempt $j failed")
            // scalastyle:on
          }
        }
        j += 1
      }
      i += 1
    }
    // scalastyle:off
    println(s"  Stopped after $i iterations, ${runTimes.sum / 1000000} ms")
    // scalastyle:on
    val best = runTimes.min
    val avg = runTimes.sum / runTimes.size
    Result(avg / 1000000.0, num / (avg / 1000.0), best / 1000000.0)
  }
}

private[spark] object QueryBenchmark {
  case class Case(
      name: String,
      fn: Benchmark.Timer => Boolean,
      numIters: Int,
      prepare: () => Unit = () => { },
      cleanup: () => Unit = () => { })

  var firstRandomValue = getFirstRandomValue(10)
  var secondRandomValue = getSecondRandomValue(10)
  def setRandomValues(valuesPerIteration: Long) : Unit = {
    firstRandomValue = getFirstRandomValue(valuesPerIteration)
    secondRandomValue = getSecondRandomValue(valuesPerIteration)
  }
  def getFirstRandomValue(valuesPerIteration: Long) : Long =
    ThreadLocalRandom.current().nextLong(valuesPerIteration)
  def getSecondRandomValue(valuesPerIteration: Long) : Long =
    ThreadLocalRandom.current().nextLong(valuesPerIteration)
}
