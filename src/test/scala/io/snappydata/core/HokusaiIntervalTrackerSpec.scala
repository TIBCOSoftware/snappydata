package io.snappydata.core

import org.apache.spark.sql.execution.{CMSParams, Hokusai}

import scala.io.Source
import org.scalatest._
import Inspectors._
import scala.collection.Map
import scala.collection.SortedSet
import scala.math.abs
import io.snappydata.util.NumberUtils

/**
 * Spec for Hokusai TimeAggregation
 */
class HokusaiIntervalTrackerSpec extends FlatSpec with Matchers {
  val SEED = 123 // Do NOT use 1 for a Seed: I think makes streamlib CMS hashing degenerate!
  val MAX_J = 8 // Number of m(j) we are keeping during tests (numSketches)
  val MAX_TIME = math.pow(2, MAX_J).asInstanceOf[Int]
  
  val FIVE_ONES = Array(1L, 1L, 1L, 1L, 1L) // data we count at each epoch
  val TEST_ACCURATE_INTERVALS = 8888
  val TEST_APRROXIMATE_INTERVALS_1 = 500
  val TEST_APRROXIMATE_INTERVALS_2 = 20
  val TEST_NUM_KEYS = 100000

  /////////////////////////////////////////////////////////////////////////////
  "Hokusai interval conversion between last nth interval to interval from begining " should "be correct" in {
    val cmsParams = CMSParams(512, 7, SEED)
    val h = new Hokusai[Int](cmsParams, 1, 0)
    val intervalsToAdd = 100
    for (i <- 1 to intervalsToAdd) {
      h.addEpochData(Map[Int, Long](1 -> 7))
    }
    //3rd interval from last should be 98th from begining 
    h.taPlusIa.convertIntervalBySwappingEnds(3) should be(98)

    //last interval should be 100th from begining
    h.taPlusIa.convertIntervalBySwappingEnds(1) should be(100)

  }

  //////////////////////////////////////////////////////////////////////////////

  "Hokusai Linked Interval" should "be correct after n intervals" in {
    /// 7, 14 ,  28, 35, 42, 49, 56, 63, 70, 77, 84, 91
    val cmsParams = CMSParams(512, 7, SEED)
    val h = new Hokusai[Int](cmsParams, 1, 0)
    h.addEpochData(Map[Int, Long](1 -> 7)) // 1st interval

    {
      assert(h.queryTillLastNIntervals(1, 1) === Some(computeSumOfAP(7, -7, 1)))
    }

    h.addEpochData(Map[Int, Long](1 -> 14)) // 2nd interval

    {
      assert(h.queryTillLastNIntervals(1, 1) === Some(14))
      assert(h.queryTillLastNIntervals(2, 1) === Some(computeSumOfAP(14, -7, 2)))
    }

    h.addEpochData(Map[Int, Long](1 -> 21)) // 3nd interval

    {
      //After 3 interval , we should get best path of  2 ( the implicit 1 interval will always be added
      val (seq, length) = h.taPlusIa.intervalTracker.identifyBestPath(3)
      assert(length === 3)
      assert(seq.length === 1)
      assert(seq.containsSlice(Array(2)))
      assert(h.queryTillLastNIntervals(3, 1) === Some(computeSumOfAP(21, -7, 3)))
      assert(h.queryTillLastNIntervals(1, 1) === Some(computeSumOfAP(21, -7, 1)))
      assert(h.queryTillLastNIntervals(2, 1) === Some(computeSumOfAP(21, -7, 2)))

    }
    //After 4 intervals
    h.addEpochData(Map[Int, Long](1 -> 28)) // 4th interval

    {
      //After 4 interval , we should get best path of 1, 2 ( the implicit 1 interval will always be added
      val (seq, length) = h.taPlusIa.intervalTracker.identifyBestPath(4)
      assert(length === 4)
      assert(seq.length === 2)
      assert(seq.containsSlice(Array(1, 2)))
      assert(h.queryTillLastNIntervals(4, 1) === Some(computeSumOfAP(28, -7, 4)))
      assert(h.queryTillLastNIntervals(1, 1) === Some(computeSumOfAP(28, -7, 1)))
      assert(h.queryTillLastNIntervals(2, 1) === Some(computeSumOfAP(28, -7, 2)))

    }

    //After 5 intervals
    h.addEpochData(Map[Int, Long](1 -> 35)) // 5th interval

    {
      //After 5 interval , we should get best path of  4 ( the implicit 1 interval will always be added
      val (seq, length) = h.taPlusIa.intervalTracker.identifyBestPath(5)
      assert(length === 5)
      assert(seq.length === 1)
      assert(seq.containsSlice(Array(4)))
      assert(h.queryTillLastNIntervals(5, 1) === Some(computeSumOfAP(35, -7, 5)))
      assert(h.queryTillLastNIntervals(1, 1) === Some(computeSumOfAP(35, -7, 1)))
      assert(h.queryTillLastNIntervals(2, 1) === Some(computeSumOfAP(35, -7, 2)))
      assert(h.queryTillLastNIntervals(3, 1) === Some(computeSumOfAP(35, -7, 3)))
    }

    //After 6 intervals
    h.addEpochData(Map[Int, Long](1 -> 42)) // 6th interval

    {
      //After 6 interval , we should get best path of  1,4 ( the implicit 1 interval will always be added
      val (seq, length) = h.taPlusIa.intervalTracker.identifyBestPath(6)
      assert(length === 6)
      assert(seq.length === 2)
      assert(seq.containsSlice(Array(1, 4)))
      assert(h.queryTillLastNIntervals(6, 1) === Some(computeSumOfAP(42, -7, 6)))
      assert(h.queryTillLastNIntervals(1, 1) === Some(computeSumOfAP(42, -7, 1)))
      assert(h.queryTillLastNIntervals(2, 1) === Some(computeSumOfAP(42, -7, 2)))
      assert(h.queryTillLastNIntervals(4, 1) === Some(computeSumOfAP(42, -7, 4)))
    }

    //After 7 intervals
    h.addEpochData(Map[Int, Long](1 -> 49)) // 7th interval

    {
      //After 7 interval , we should get best path of  2,4 ( the implicit 1 interval will always be added
      val (seq, length) = h.taPlusIa.intervalTracker.identifyBestPath(7)
      assert(length === 7)
      assert(seq.length === 2)
      assert(seq.containsSlice(Array(2, 4)))
      assert(h.queryTillLastNIntervals(7, 1) === Some(computeSumOfAP(49, -7, 7)))
      assert(h.queryTillLastNIntervals(1, 1) === Some(computeSumOfAP(49, -7, 1)))
      assert(h.queryTillLastNIntervals(2, 1) === Some(computeSumOfAP(49, -7, 2)))
      assert(h.queryTillLastNIntervals(3, 1) === Some(computeSumOfAP(49, -7, 3)))
    }

    //After 8 intervals
    h.addEpochData(Map[Int, Long](1 -> 56))

    {
      //After 8 interval , we should get best path of  1,2,4 ( the implicit 1 interval will always be added
      val (seq, length) = h.taPlusIa.intervalTracker.identifyBestPath(8)
      assert(length === 8)
      assert(seq.length === 3)
      assert(seq.containsSlice(Array(1, 2, 4)))
      assert(h.queryTillLastNIntervals(8, 1) === Some(computeSumOfAP(56, -7, 8)))
      assert(h.queryTillLastNIntervals(1, 1) === Some(computeSumOfAP(56, -7, 1)))
      assert(h.queryTillLastNIntervals(2, 1) === Some(computeSumOfAP(56, -7, 2)))
      assert(h.queryTillLastNIntervals(4, 1) === Some(computeSumOfAP(56, -7, 4)))
    }

    //After 9 intervals
    h.addEpochData(Map[Int, Long](1 -> 63))

    {
      //After 9 interval , we should get best path of  8 ( the implicit 1 interval will always be added
      val (seq, length) = h.taPlusIa.intervalTracker.identifyBestPath(9)
      assert(length === 9)
      assert(seq.length === 1)
      assert(seq.containsSlice(Array(8)))
      assert(h.queryTillLastNIntervals(9, 1) === Some(computeSumOfAP(63, -7, 9)))
      assert(h.queryTillLastNIntervals(1, 1) === Some(computeSumOfAP(63, -7, 1)))
      assert(h.queryTillLastNIntervals(2, 1) === Some(computeSumOfAP(63, -7, 2)))
      assert(h.queryTillLastNIntervals(3, 1) === Some(computeSumOfAP(63, -7, 3)))
      assert(h.queryTillLastNIntervals(5, 1) === Some(computeSumOfAP(63, -7, 5)))

    }

    //After 10 intervals
    h.addEpochData(Map[Int, Long](1 -> 70))

    {
      //After 10 interval , we should get best path of 1, 8 ( the implicit 1 interval will always be added
      val (seq, length) = h.taPlusIa.intervalTracker.identifyBestPath(10)
      assert(length === 10)
      assert(seq.length === 2)
      assert(seq.containsSlice(Array(1, 8)))
      assert(h.queryTillLastNIntervals(10, 1) === Some(computeSumOfAP(70, -7, 10)))
      assert(h.queryTillLastNIntervals(2, 1) === Some(computeSumOfAP(70, -7, 2)))
      assert(h.queryTillLastNIntervals(4, 1) === Some(computeSumOfAP(70, -7, 4)))
      assert(h.queryTillLastNIntervals(6, 1) === Some(computeSumOfAP(70, -7, 6)))
      assert(h.queryTillLastNIntervals(1, 1) === Some(computeSumOfAP(70, -7, 1)))

    }

    //After 11 intervals
    h.addEpochData(Map[Int, Long](1 -> 77))

    {
      //After 11 interval , we should get best path of 2, 8 ( the implicit 1 interval will always be added
      val (seq, length) = h.taPlusIa.intervalTracker.identifyBestPath(11)
      assert(length === 11)
      assert(seq.length === 2)
      assert(seq.containsSlice(Array(2, 8)))
      assert(h.queryTillLastNIntervals(11, 1) === Some(computeSumOfAP(77, -7, 11)))
      assert(h.queryTillLastNIntervals(1, 1) === Some(computeSumOfAP(77, -7, 1)))
      assert(h.queryTillLastNIntervals(2, 1) === Some(computeSumOfAP(77, -7, 2)))
      assert(h.queryTillLastNIntervals(3, 1) === Some(computeSumOfAP(77, -7, 3)))
      assert(h.queryTillLastNIntervals(7, 1) === Some(computeSumOfAP(77, -7, 7)))

    }

    //After 12 intervals
    h.addEpochData(Map[Int, Long](1 -> 84))

    {
      //After 12 interval , we should get best path of 1,2, 8 ( the implicit 1 interval will always be added
      val (seq, length) = h.taPlusIa.intervalTracker.identifyBestPath(12)
      assert(length === 12)
      assert(seq.length === 3)
      assert(seq.containsSlice(Array(1, 2, 8)))
      assert(h.queryTillLastNIntervals(12, 1) === Some(computeSumOfAP(84, -7, 12)))
      assert(h.queryTillLastNIntervals(1, 1) === Some(computeSumOfAP(84, -7, 1)))
      assert(h.queryTillLastNIntervals(2, 1) === Some(computeSumOfAP(84, -7, 2)))
      assert(h.queryTillLastNIntervals(4, 1) === Some(computeSumOfAP(84, -7, 4)))
      /* assert(h.queryTillLastNIntervals(7, 1) === Some(441))*/

    }

    //After 13 intervals
    h.addEpochData(Map[Int, Long](1 -> 91))

    {
      //After 13 interval , we should get best path of 4, 8 ( the implicit 1 interval will always be added
      val (seq, length) = h.taPlusIa.intervalTracker.identifyBestPath(13)
      assert(length === 13)
      assert(seq.length === 2)
      assert(seq.containsSlice(Array(4, 8)))
      assert(h.queryTillLastNIntervals(13, 1) === Some(computeSumOfAP(91, -7, 13)))
      assert(h.queryTillLastNIntervals(2, 1) === Some(computeSumOfAP(91, -7, 2)))
      assert(h.queryTillLastNIntervals(3, 1) === Some(computeSumOfAP(91, -7, 3)))
      assert(h.queryTillLastNIntervals(5, 1) === Some(computeSumOfAP(91, -7, 5)))
      /* assert(h.queryTillLastNIntervals(7, 1) === Some(441))*/

    }

    //After 14 intervals
    h.addEpochData(Map[Int, Long](1 -> 98))

    {
      //After 14 interval , we should get best path of1, 4, 8 ( the implicit 1 interval will always be added
      val (seq, length) = h.taPlusIa.intervalTracker.identifyBestPath(14)
      assert(length === 14)
      assert(seq.length === 3)
      assert(seq.containsSlice(Array(1, 4, 8)))
      assert(h.queryTillLastNIntervals(14, 1) === Some(computeSumOfAP(98, -7, 14)))
      assert(h.queryTillLastNIntervals(2, 1) === Some(computeSumOfAP(98, -7, 2)))
      assert(h.queryTillLastNIntervals(4, 1) === Some(computeSumOfAP(98, -7, 4)))
      assert(h.queryTillLastNIntervals(6, 1) === Some(computeSumOfAP(98, -7, 6)))

    }

    //After 15 intervals
    h.addEpochData(Map[Int, Long](1 -> 105))

    {
      //After 15 interval , we should get best path of 2, 4, 8 ( the implicit 1 interval will always be added
      val (seq, length) = h.taPlusIa.intervalTracker.identifyBestPath(15)
      assert(length === 15)
      assert(seq.length === 3)
      assert(seq.containsSlice(Array(2, 4, 8)))
      assert(h.queryTillLastNIntervals(15, 1) === Some(computeSumOfAP(105, -7, 15)))
      assert(h.queryTillLastNIntervals(2, 1) === Some(computeSumOfAP(105, -7, 2)))
      assert(h.queryTillLastNIntervals(3, 1) === Some(computeSumOfAP(105, -7, 3)))
      assert(h.queryTillLastNIntervals(7, 1) === Some(computeSumOfAP(105, -7, 7)))

    }

    //After 16 intervals
    h.addEpochData(Map[Int, Long](1 -> 112))

    {
      //After 16 interval , we should get best path of 1, 2, 4, 8 ( the implicit 1 interval will always be added
      val (seq, length) = h.taPlusIa.intervalTracker.identifyBestPath(16)
      assert(length === 16)
      assert(seq.length === 4)
      assert(seq.containsSlice(Array(1, 2, 4, 8)))
      assert(h.queryTillLastNIntervals(16, 1) === Some(computeSumOfAP(112, -7, 16)))
      assert(h.queryTillLastNIntervals(2, 1) === Some(computeSumOfAP(112, -7, 2)))
      assert(h.queryTillLastNIntervals(4, 1) === Some(computeSumOfAP(112, -7, 4)))
      assert(h.queryTillLastNIntervals(8, 1) === Some(computeSumOfAP(112, -7, 8)))

    }

  }

  ////////////////////////////////////////////////////////////////
  "Hokusai queries for n intervals" should "be correct for all paths" in {
    /// 7, 14 ,  28, 35, 42, 49, 56, 63, 70, 77, 84, 91
    val cmsParams = CMSParams(1024, 7, SEED)
    val h = new Hokusai[Int](cmsParams, 1, 0)
    (1 to TEST_ACCURATE_INTERVALS) foreach {
      i =>
        val count = 7 * i;
        h.addEpochData(Map[Int, Long](1 -> count))
        val possiblePaths = h.taPlusIa.intervalTracker.getAllPossiblePaths
        possiblePaths.foreach {
          path =>
            val totalInterval = path.aggregate[Long](0)(_ + _, _ + _)
            assert(h.queryTillLastNIntervals(totalInterval.asInstanceOf[Int], 1)
              === Some(computeSumOfAP(count, -7, totalInterval.asInstanceOf[Int])))
        }
    }
  }

  ////////////////////////////////////////////////////////////////
  "Hokusai queries for n past intervals using interpolation" should "be approximately correct " in {
    /// 7, 14 ,  28, 35, 42, 49, 56, 63, 70, 77, 84, 91
    val ERROR_PERCENTAGE = 0
    val cmsParams = CMSParams(1024, 7, SEED)
    val h = new Hokusai[Int](cmsParams, 1, 0)
    (1 to TEST_APRROXIMATE_INTERVALS_1) foreach {
      i =>
        val count = 7 * i;
        h.addEpochData(Map[Int, Long](1 -> count))
        val possiblePaths = h.taPlusIa.intervalTracker.getAllPossiblePaths
        val accurateIntervals = possiblePaths.map(_.aggregate(0)(_ + _, _ + _))
        val sortedAccurateIntervals = SortedSet(accurateIntervals: _*)(Ordering.Int)
        var start = 1
        sortedAccurateIntervals.foreach(x =>
          {
            start until x foreach {
              i =>
                if (h.queryTillLastNIntervals(i, 1)
                  != Some(computeSumOfAP(count, -7, i))) {
                  val approx = h.queryTillLastNIntervals(i, 1).get
                  val exact = computeSumOfAP(count, -7, i)
                  val error = (abs(exact - approx) * 100f) / exact
                  if (error > ERROR_PERCENTAGE) {
                    fail("error more than " + ERROR_PERCENTAGE + "%, with error=" + error + "%")
                  }

                }
            }
            start = x + 1
          })
    }
  }

  /////////////////////////////////////////////////////////////////

  ////////////////////////////////////////////////////////////////
  "Hokusai queries for n past intervals using interpolation for " + TEST_NUM_KEYS + " keys" should "be approximately correct " in {
    /// 7, 14 ,  28, 35, 42, 49, 56, 63, 70, 77, 84, 91
    val cmsParams = CMSParams(NumberUtils.nearestPowerOf2GE(816096), 10, SEED)
    val h = new Hokusai[Int](cmsParams, 1, 0)
    val ERROR_PERCENTAGE = 25
    (1 to TEST_APRROXIMATE_INTERVALS_2) foreach {
      k =>
        val dataMap = scala.collection.mutable.Map[Int, Long]()
        1 to TEST_NUM_KEYS foreach {
          j =>
            val elem = j + (k - 1) * j
            dataMap += (j -> elem)
        }

        h.addEpochData(dataMap)
        val possiblePaths = h.taPlusIa.intervalTracker.getAllPossiblePaths
        val accurateIntervals = possiblePaths.map(_.aggregate(0)(_ + _, _ + _))
        val sortedAccurateIntervals = SortedSet(accurateIntervals: _*)(Ordering.Int)
        var start = 1
        sortedAccurateIntervals.foreach(x =>
          {
            val randomKey = new scala.util.Random(41)

            start until x foreach {
              i =>
                val keyToQuery = randomKey.nextInt(TEST_NUM_KEYS)
                if (keyToQuery > 0) {
                  val lastElemOfAP = keyToQuery + (k - 1) * keyToQuery

                  val exactFreq = computeSumOfAP(lastElemOfAP, -1 * keyToQuery, i)

                  val freqFromQuery = h.queryTillLastNIntervals(i, keyToQuery).get
                 // System.out.println("expected frequency=" + exactFreq + ", hokusai count= " + freqFromQuery)
                  if (freqFromQuery != exactFreq) {
                    val errorPercentage = (abs(exactFreq - freqFromQuery) * 100f) / exactFreq
                    if (errorPercentage > ERROR_PERCENTAGE) {
                      fail("error more than " + ERROR_PERCENTAGE + "%. With the error ="
                        + errorPercentage + "%. This occurred when querying past last " + i
                        + " intervals, with the total intervals so far=" + k)
                    }

                  }
                }
            }
            start = x + 1
          })
    }
  }

  /////////////////////////////////////////////////////////////////
  "Hokusai queries at a given time in past  using interpolation for keys" should "be approximately correct " in {
    /// 7, 14 ,  28, 35, 42, 49, 56, 63, 70, 77, 84, 91
    val cmsParams = CMSParams(NumberUtils.nearestPowerOf2GE(1024), 5, SEED)
    val h = new Hokusai[Int](cmsParams, 1, 0)
    val KEYS = 100
    val ERROR_PERCENTAGE = 0
    (1 to TEST_APRROXIMATE_INTERVALS_1) foreach {
      k =>
        val dataMap = scala.collection.mutable.Map[Int, Long]()
        1 to KEYS foreach {
          j =>
            val elem = j + (k - 1) * j
            dataMap += (j -> elem)
        }

        h.addEpochData(dataMap)
    }
    //For each interval in past check some keys
    (1 to TEST_APRROXIMATE_INTERVALS_1) foreach {
      k =>
        val randomKey = new scala.util.Random(41)
        1 to 20 foreach {
          j =>
            val keyToQuery = randomKey.nextInt(KEYS)
            if (keyToQuery > 0) {
              val lastElemOfAP = keyToQuery + (k - 1) * keyToQuery
              val countForInterval = h.queryAtTime(k - 1, keyToQuery).get
              if (countForInterval != lastElemOfAP) {
                val errorPercentage = (abs(countForInterval - lastElemOfAP) * 100f) / lastElemOfAP
                if (errorPercentage > ERROR_PERCENTAGE) {
                  fail("error more than " + ERROR_PERCENTAGE + "%. With the error ="
                    + errorPercentage + "%. This occurred when querying at time " + (k - 1)
                    + " intervals, with the total intervals so far=" + k)
                }

              }
            }
        }

    }

  }

  /////////////////////////////////////////////////////////////////
  "Hokusai queries between two time intervals in past  using interpolation for keys" should "be approximately correct " in {
    /// 7, 14 ,  28, 35, 42, 49, 56, 63, 70, 77, 84, 91
    val cmsParams = CMSParams(NumberUtils.nearestPowerOf2GE(8192), 5, SEED)
    val h = new Hokusai[Int](cmsParams, 10, 0)

    //For each interval in past check some keys

    h.addEpochData(Map[Int, Long](1 -> 7)) // 1 interval /10
    h.addEpochData(Map[Int, Long](1 -> 14)) // 2 interval /20
    h.addEpochData(Map[Int, Long](1 -> 21)) // 3 interval /30
    h.addEpochData(Map[Int, Long](1 -> 28)) // 4 interval /40
    h.addEpochData(Map[Int, Long](1 -> 35)) // 5 interval /50
    h.addEpochData(Map[Int, Long](1 -> 42)) // 6 interval /60
    h.addEpochData(Map[Int, Long](1 -> 49)) // 7 interval /70
    h.addEpochData(Map[Int, Long](1 -> 56)) // 8 interval /80
    h.addEpochData(Map[Int, Long](1 -> 63)) // 9 interval /90
    h.addEpochData(Map[Int, Long](1 -> 70)) // 10 interval /100
    h.addEpochData(Map[Int, Long](1 -> 77)) // 11 interval /110
    h.addEpochData(Map[Int, Long](1 -> 84)) // 12 interval /120
    h.addEpochData(Map[Int, Long](1 -> 91)) // 13 interval /130
    h.addEpochData(Map[Int, Long](1 -> 98)) // 14 interval /140
    h.addEpochData(Map[Int, Long](1 -> 105)) // 15 interval /150
    h.addEpochData(Map[Int, Long](1 -> 112)) // 16 interval /160
    h.addEpochData(Map[Int, Long](1 -> 119)) // 17 interval /170
    h.addEpochData(Map[Int, Long](1 -> 126)) // 18 interval /180

    {
      assert(h.queryBetweenTime(13, 23, 1) === Some(14 + 21))
      assert(h.queryBetweenTime(13, 18, 1) === Some(14))
      assert(h.queryBetweenTime(0, 35, 1) === Some(7 + 14 + 21 + 28))
      assert(h.queryBetweenTime(15, 113, 1) === Some(14 + 21 + 28 + 35 + 42 + 49 + 56 + 63 + 70
        + 77 + 84))
        
      assert(h.queryBetweenTime(175, 178, 1) === Some(126))  
    }

  }

  /////////////////////////////////////////////////////////////////
  "Hokusai queries between two time intervals (generic test) in past  using interpolation for keys" should "be approximately correct " in {
    /// 7, 14 ,  28, 35, 42, 49, 56, 63, 70, 77, 84, 91
    val cmsParams = CMSParams(NumberUtils.nearestPowerOf2GE(8192),7, SEED)
    val h = new Hokusai[Int](cmsParams, 10, 0)
    val KEYS = 1
    val ERROR_PERCENTAGE =0
    val TEST_NUM_INTERVALS = 1000
    val NUM_RUN = 1000
    (1 to TEST_NUM_INTERVALS) foreach {
      k =>
        val count = 7 * k;
        //System.out.println("Interval number ="+k + " count added ="+ count)
        h.addEpochData(Map[Int, Long](1 -> count))
    }
    //For each interval in past check some keys
    val randomKey = new scala.util.Random(41)

    (1 to NUM_RUN) foreach {
      k =>
        val one = randomKey.nextInt(TEST_NUM_INTERVALS)
        val two = randomKey.nextInt(TEST_NUM_INTERVALS)
        val t1 = one * 10
        val t2 = two * 10
        val min = math.min(one, two)
        val max = math.max(one, two)
        val s1 = computeSumOfAP(7, 7, min)
        val s2 = computeSumOfAP(7, 7, max + 1)
        
        
        val estimate = h.queryBetweenTime(t1, t2, 1).get
        val expected = Math.abs(s2 - s1)
        if (estimate != expected) {
          val errorPercentage = (abs(estimate - expected) * 100f) / expected
          if (errorPercentage > ERROR_PERCENTAGE) {
            fail("error more than " + ERROR_PERCENTAGE + "%. With the error ="
              + errorPercentage + "%. expected value =" + expected + "; actual value ="+ estimate
            + " interval queried = " +one + ","+ two  
            )
          }

        }
       /* val estimate1 = h.queryBetweenTime(85, 85, 1)
         val expected1 = Math.abs(computeSumOfAP(7, 7, 9) - computeSumOfAP(7, 7, 17))
         System.out.println("estimate1 = "+ estimate1 + " expceted1 = "+ expected1)
        
         val estimate2 = h.queryBetweenTime(155, 195, 1)
         val expected2 = Math.abs(computeSumOfAP(7, 7, 15) - computeSumOfAP(7, 7, 20))
         System.out.println("estimate2 = "+ estimate2 + " expceted2 = "+ expected2)*/
    }

  }

  ////////////////////////////////////////////////////////////////
  private def computeSumOfAP(a: Int, d: Int, n: Int): Int = {
    val x: Int = (n * (2 * a + (n - 1) * d)).asInstanceOf[Int]
    x / 2
  }
}
