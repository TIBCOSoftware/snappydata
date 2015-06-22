package io.snappydata.core

import scala.io.Source
import org.scalatest._
import Inspectors._  // picks up forAll () {}

/**
 * Spec for Hokusai TimeAggregation
 */
class HokusaiSpec extends FlatSpec with Matchers {
  val SEED = 123 // Do NOT use 1 for a Seed: I think makes streamlib CMS hashing degenerate!
  val MAX_J = 8  // Number of m(j) we are keeping during tests (numSketches)
  val MAX_TIME = math.pow(2, MAX_J).asInstanceOf[Int]

  val FIVE_ONES = Array(1L, 1L, 1L, 1L, 1L) // data we count at each epoch

  val cmsParams = CMSParams(128, 7, SEED)

/*
  "TimeEpoch.timePeriodForJ()" should "handle bad input values" in {
    val te = new TimeEpoch()
    te.timePeriodForJ(-1L, 0) should be (None)
    te.timePeriodForJ(0, -1) should be (None)
  }*/

  /*
  it should "calculate start and end timestamps for m(j)" in {
    // We test for time = 0 to 8, which includes 4 powers of two, so we should be ok
    // on lower corner cases and the pattern should be clear from this.
    //
    // At time 0, all m(j) should be in range 0-0, or None
    val te = new TimeEpoch()
    var t = 0
    (0 until 33) foreach { j => te.timePeriodForJ(t, j) should be (None) }

    t = 1
    te.timePeriodForJ(t, 0) should be (Some((0,1)))
    (1 until 33) foreach { j => te.timePeriodForJ(t, j) should be (None) }

    t = 2
    te.timePeriodForJ(t, 0) should be (Some((1,2)))
    te.timePeriodForJ(t, 1) should be (Some((0,2)))
    (2 until 33) foreach { j => te.timePeriodForJ(t, j) should be (None) }

    t = 3
    te.timePeriodForJ(t, 0) should be (Some((2,3)))
    te.timePeriodForJ(t, 1) should be (Some((0,2)))
    (2 until 33) foreach { j => te.timePeriodForJ(t, j) should be (None) }


    t = 4
    te.timePeriodForJ(t, 0) should be (Some((3,4)))
    te.timePeriodForJ(t, 1) should be (Some((2,4)))
    te.timePeriodForJ(t, 2) should be (Some((0,4)))
    (3 until 33) foreach { j => te.timePeriodForJ(t, j) should be (None) }

    t = 5
    te.timePeriodForJ(t, 0) should be (Some((4,5)))
    te.timePeriodForJ(t, 1) should be (Some((2,4)))
    te.timePeriodForJ(t, 2) should be (Some((0,4)))
    (3 until 33) foreach { j => te.timePeriodForJ(t, j) should be (None) }

    t = 6
    te.timePeriodForJ(t, 0) should be (Some((5,6)))
    te.timePeriodForJ(t, 1) should be (Some((4,6)))
    te.timePeriodForJ(t, 2) should be (Some((0,4)))
    (3 until 33) foreach { j => te.timePeriodForJ(t, j) should be (None) }

    t = 7
    te.timePeriodForJ(t, 0) should be (Some((6,7)))
    te.timePeriodForJ(t, 1) should be (Some((4,6)))
    te.timePeriodForJ(t, 2) should be (Some((0,4)))
    (3 until 33) foreach { j => te.timePeriodForJ(t, j) should be (None) }

    t = 8
    te.timePeriodForJ(t, 0) should be (Some((7,8)))
    te.timePeriodForJ(t, 1) should be (Some((6,8)))
    te.timePeriodForJ(t, 2) should be (Some((4,8)))
    te.timePeriodForJ(t, 3) should be (Some((0,8)))
    (4 until 33) foreach { j => te.timePeriodForJ(t, j) should be (None) }
  }

  it should "work with time windowSize > 1"
  it should "be None for all j, before any increment (for time windowSize > 1)"

*/
  /////////////////////////////////////////////////////////////////////////////
/*
  "TimeEpoch.jForTimestamp" should "be None for all j, before any increment" in {
    val te = new TimeEpoch()
    forAll (-1 until 10) { t => te.timestampToInterval(t) should be (None) }
  }

  it should "be correct for all j, after increment" in {
    val te = new TimeEpoch()

    // After 1 increment
    te.increment()
    te.timestampToInterval(0) should be (Some(0))
    forAll (1 until 10) { t=> te.timestampToInterval(t) should be (None) }

    // After 2 increments
    te.increment()
    te.timestampToInterval(0) should be (Some(1))
    te.timestampToInterval(1) should be (Some(0))
    forAll (2 until 10) { t=> te.timestampToInterval(t) should be (None) }

    // After 3 increments
    te.increment()
    te.timestampToInterval(0) should be (Some(1))
    te.timestampToInterval(1) should be (Some(1))
    te.timestampToInterval(2) should be (Some(0))
    forAll (3 until 10) { t=> te.timestampToInterval(t) should be (None) }

    // After 4 increments
    te.increment()
    te.timestampToInterval(0) should be (Some(2))
    te.timestampToInterval(1) should be (Some(2))
    te.timestampToInterval(2) should be (Some(1))
    te.timestampToInterval(3) should be (Some(0))
    forAll (4 until 10) { t=> te.timestampToInterval(t) should be (None) }
  }
*/
  //////////////////////////////////////////////////////////////////////////////
/*
  "TimeAggregation" should "report None before first time increment" in {
    val ta : TimeAggregation[Long] = null// new TimeAggregation[Long](null,cmsParams)
    forAll (0 until MAX_TIME) { t =>
      forAll (0L until 100L) { k =>
        ta.queryTillTime(t, k) should be (None)
      }
    }
  }
*/

  "Hokusai TimeAggregation" should "be correct after n epochs" in {
    val h = new Hokusai[Long](cmsParams,1,0)
    // Every epoch, we will count 5 occurrences of 1L
    // So, each successive m(j) should have 2^j * 5?

    // 1st Epoch (0):  total num 1s seen: 5
    h.addEpochData(FIVE_ONES)
    h.queryTillTime(0, 1L) should be (Some(5))  // m(0): [0,1)
    (1 until MAX_TIME) foreach { h.queryTillTime(_, 1L) should be (None) }

    // 2nd Epoch (1):  total num 1s seen: 10
    h.addEpochData(FIVE_ONES)
    h.queryTillTime(0, 1L) should be (Some(10)) // m(1): [0,2)  // 5
    h.queryTillTime(1, 1L) should be (Some(5))  // m(0): [2,3)  // 10
    (2 until MAX_TIME) foreach { h.queryTillTime(_, 1L) should be (None) }

    // 3rd Epoch (2):  total num 1s seen: 15
    h.addEpochData(FIVE_ONES)
    h.queryTillTime(0, 1L) should be (Some(15))  // m(1): [0,2)  // 5
    h.queryTillTime(1, 1L) should be (Some(10))  // m(1): [0,2)
    h.queryTillTime(2, 1L) should be (Some(5))   // m(0): [2,3)  // None
    (3 until MAX_TIME) foreach { h.queryTillTime(_, 1L) should be (None) }

    // 4th Epoch (3):  total num 1s seen: 20
    h.addEpochData(FIVE_ONES)
    h.queryTillTime(0, 1L) should be (Some(20))  // m(2): [0,4)
    h.queryTillTime(1, 1L) should be (Some(15))  // m(2): [0,4)
    h.queryTillTime(2, 1L) should be (Some(10))  // m(1): [2,4)
    h.queryTillTime(3, 1L) should be (Some(5))   // m(0): [3,4)
    (4 until MAX_TIME) foreach { h.queryTillTime(_, 1L) should be (None) }

    // 5th Epoch (4):  total num 1s seen: 25
    h.addEpochData(FIVE_ONES)
    h.queryTillTime(0, 1L) should be (Some(25))  // m(2): [0,4)
    h.queryTillTime(1, 1L) should be (Some(20))  // m(2): [0,4)
    h.queryTillTime(2, 1L) should be (Some(15))  // m(1): [2,4)
    h.queryTillTime(3, 1L) should be (Some(10))  // m(1): [2,4)
    h.queryTillTime(4, 1L) should be (Some(5))   // m(0): [4,5)
    (5 until MAX_TIME) foreach { h.queryTillTime(_, 1L) should be (None) }

    // 6th Epoch (5):  total num 1s seen: 30
    h.addEpochData(FIVE_ONES)
    h.queryTillTime(0, 1L) should be (Some(30))  // m(2): [0,4)
    h.queryTillTime(1, 1L) should be (Some(25))  // m(2): [0,4)
    h.queryTillTime(2, 1L) should be (Some(20))  // m(2): [0,4)
    h.queryTillTime(3, 1L) should be (Some(15))  // m(2): [0,4)
    h.queryTillTime(4, 1L) should be (Some(10))  // m(1): [4,6)
    h.queryTillTime(5, 1L) should be (Some(5))   // m(0): [5,6)
    (6 until MAX_TIME) foreach { h.queryTillTime(_, 1L) should be (None) }

    // 7th Epoch (6):  total num 1s seen: 35
    h.addEpochData(FIVE_ONES)
    h.queryTillTime(0, 1L) should be (Some(35))  // m(2): [0,4)
    h.queryTillTime(1, 1L) should be (Some(30))  // m(2): [0,4)
    h.queryTillTime(2, 1L) should be (Some(25))  // m(2): [0,4)
    h.queryTillTime(3, 1L) should be (Some(20))  // m(2): [0,4)
    h.queryTillTime(4, 1L) should be (Some(15))  // m(1): [4,6)
    h.queryTillTime(5, 1L) should be (Some(10))  // m(1): [4,6)
    h.queryTillTime(6, 1L) should be (Some(5))   // m(0): [6,7)
    (7 until MAX_TIME) foreach { h.queryTillTime(_, 1L) should be (None) }

    // 8th Epoch (7):  total num 1s seen: 40
    h.addEpochData(FIVE_ONES)
    h.queryTillTime(0, 1L) should be (Some(40))  // m(2): [0,8)
    h.queryTillTime(1, 1L) should be (Some(35))  // m(2): [0,8)
    h.queryTillTime(2, 1L) should be (Some(30))  // m(2): [0,8)
    h.queryTillTime(3, 1L) should be (Some(25))  // m(2): [0,8)
    h.queryTillTime(4, 1L) should be (Some(20))  // m(2): [4,8)
    h.queryTillTime(5, 1L) should be (Some(15))  // m(2): [4,8)
    h.queryTillTime(6, 1L) should be (Some(10))  // m(1): [6,8)
    h.queryTillTime(7, 1L) should be (Some(5))   // m(0): [7,8)
    (8 until MAX_TIME) foreach { h.queryTillTime(_, 1L) should be (None) }

    // 9th Epoch (8):  total num 1s seen: 45
    h.addEpochData(FIVE_ONES)
    h.queryTillTime(0, 1L) should be (Some(45))  // m(2): [0,8)
    h.queryTillTime(1, 1L) should be (Some(40))  // m(2): [0,8)
    h.queryTillTime(2, 1L) should be (Some(35))  // m(2): [0,8)
    h.queryTillTime(3, 1L) should be (Some(30))  // m(2): [0,8)
    h.queryTillTime(4, 1L) should be (Some(25))  // m(2): [4,8)
    h.queryTillTime(5, 1L) should be (Some(20))  // m(2): [4,8)
    h.queryTillTime(6, 1L) should be (Some(15))  // m(1): [6,8)
    h.queryTillTime(7, 1L) should be (Some(10))   // m(1): [6,8)
    h.queryTillTime(8, 1L) should be (Some(5))   // m(0): [8,9)
    (9 until MAX_TIME) foreach { h.queryTillTime(_, 1L) should be (None) }


    // At end, we want to make sure we haven't counted anything other than 1s
    // So, for all valid time frames, we should get Some(0), for invalid
    // time frames we should get None
    (0 until 8) foreach { j =>
      (2L until 50L) foreach {n => h.queryTillTime(j, n) should be (Some(0)) }}

    (9 until 33) foreach { j =>
      (2L until 50L) foreach {n => h.queryTillTime(j, n) should be (None) }}
  }

}
