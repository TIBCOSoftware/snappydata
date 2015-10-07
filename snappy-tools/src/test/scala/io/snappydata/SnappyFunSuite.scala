package io.snappydata

import org.apache.spark.Logging
import org.scalatest.{FunSuite, Outcome}

/**
 * Base abstract class for all snappydata tests
 * similar to SparkFunSuite.
 *
 * Created by soubhikc on 6/10/15.
 */
private[snappydata] abstract class SnappyFunSuite extends FunSuite with Logging {

  /**
   * Copied from SparkFunSuite.
   *
   * Log the suite name and the test name before and after each test.
   *
   * Subclasses should never override this method. If they wish to run
   * custom code before and after each test, they should mix in the
   * {{org.scalatest.BeforeAndAfter}} trait instead.
   */
  final protected override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("io.snappydata", "i.sd")
    try {
      logInfo(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      test()
    } finally {
      logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }

}
