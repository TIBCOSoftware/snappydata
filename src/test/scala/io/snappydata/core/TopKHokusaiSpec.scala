package io.snappydata.core

import org.apache.spark.sql.collection.BoundedSortedSet
import org.apache.spark.sql.execution.{TopKHokusai, CMSParams}

import scala.io.Source
import org.scalatest._
import Inspectors._
import scala.collection.Map
import scala.collection.SortedSet
import scala.math.abs
import io.snappydata.util.NumberUtils
import org.apache.spark.sql.execution.cms.TopKCMS

/**
 * Spec for Hokusai TimeAggregation
 */
class TopKHokusaiSpec extends FlatSpec with Matchers {
  val SEED = 123 // Do NOT use 1 for a Seed: I think makes streamlib CMS hashing degenerate!
  val MAX_J = 8 // Number of m(j) we are keeping during tests (numSketches)
  val MAX_TIME = math.pow(2, MAX_J).asInstanceOf[Int]

  val FIVE_ONES = Array(1L, 1L, 1L, 1L, 1L) // data we count at each epoch
  val TEST_ACCURATE_INTERVALS = 8888
  val TEST_APRROXIMATE_INTERVALS_1 = 500
  val TEST_APRROXIMATE_INTERVALS_2 = 20
  val TEST_NUM_KEYS = 100000

  /////////////////////////////////////////////////////////////////////////////
  "TopK elements  for each interval  " should "be correct" in {
    val cmsParams = CMSParams(8192, 7, SEED)
    val topK = new TopKHokusai[Int](cmsParams, 1, 0, 5)
    //1st interval
    topK.addEpochData(Map[Int, Long](1 -> 13, 2 -> 8, 3 -> 9, 4 -> 10, 5 -> 4, 6 -> 7, 7 -> 1, 8 -> 19, 9 -> 0, 10 -> 11))

    {
      val topKCMS1: TopKCMS[Int] = topK.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[Int]]
      val top5Elements = topKCMS1.getTopK
      assert(top5Elements(0)._1 == 8)
      assert(top5Elements(0)._2 == 19)

      assert(top5Elements(1)._1 == 1)
      assert(top5Elements(1)._2 == 13)

      assert(top5Elements(2)._1 == 10)
      assert(top5Elements(2)._2 == 11)

      assert(top5Elements(3)._1 == 4)
      assert(top5Elements(3)._2 == 10)

      assert(top5Elements(4)._1 == 3)
      assert(top5Elements(4)._2 == 9)
    }
    //2nd interval

    topK.addEpochData(Map[Int, Long](10 -> 13, 8 -> 8, 7 -> 9, 9 -> 10, 3 -> 4, 5 -> 7, 4 -> 1, 1 -> 19, 2 -> 0, 6 -> 11))

    {
      val topKCMS1: TopKCMS[Int] = topK.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[Int]]
      val top5Elements = topKCMS1.getTopK
      assert(top5Elements(0)._1 == 1)
      assert(top5Elements(0)._2 == 19)

      assert(top5Elements(1)._1 == 10)
      assert(top5Elements(1)._2 == 13)

      assert(top5Elements(2)._1 == 6)
      assert(top5Elements(2)._2 == 11)

      assert(top5Elements(3)._1 == 9)
      assert(top5Elements(3)._2 == 10)

      assert(top5Elements(4)._1 == 7)
      assert(top5Elements(4)._2 == 9)
    }

    //3rd interval
    //The 3rd element should contain the topK of the last two intervals
    //1 -> 32, 2->8, 3->13, 4->11, 5->11, 6->18, 7->10, 8->27, 9->10, 10->24

    topK.addEpochData(Map[Int, Long](4 -> 13, 3 -> 8, 1 -> 9, 2 -> 10, 9 -> 4, 6 -> 7, 10 -> 1, 8 -> 19, 7 -> 0, 5 -> 11))

    {
      val topKCMS2: TopKCMS[Int] = topK.taPlusIa.ta.aggregates(2).asInstanceOf[TopKCMS[Int]]
      val top5Elements = topKCMS2.getTopK
      assert(top5Elements(0)._1 == 1)
      assert(top5Elements(0)._2 == 32)

      assert(top5Elements(1)._1 == 8)
      assert(top5Elements(1)._2 == 27)

      assert(top5Elements(2)._1 == 10)
      assert(top5Elements(2)._2 == 24)

      assert(top5Elements(3)._1 == 6)
      assert(top5Elements(3)._2 == 18)

      assert(top5Elements(4)._1 == 3)
      assert(top5Elements(4)._2 == 13)

    }

  }
  //////////////////////////////////////////////////////////////////////////////
  "TopK expansion with increased time intervals " should "be correct" in {
    val cmsParams = CMSParams(NumberUtils.nearestPowerOf2GE(8192), 7, SEED)
    val topKCount = 4
    val topK = new TopKHokusai[Int](cmsParams, 10, 0, topKCount)

    {
      //1st interval add 10 keys
      topK.addEpochData(Map[Int, Long](1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4, 5 -> 5, 6 -> 6,
        7 -> 7, 8 -> 8, 9 -> 9, 10 -> 10))
      val topKCMS = topK.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[Int]]
      assert(topKCMS.topKActual === 4)
      assert(topKCMS.topKInternal === 8)
      val topSetKeysIter = topKCMS.topkSet.iterator()
      (10 to 3) foreach { j =>
        val (key, value) = topSetKeysIter.next()
        assert(key === j)
        assert(value === j)
      }
    }

    {
      //add 2nd interval with next 10 keys
      topK.addEpochData(Map[Int, Long](11 -> 11, 12 -> 12, 13 -> 13, 14 -> 14, 15 -> 15,
        16 -> 16, 17 -> 17, 18 -> 18, 19 -> 19, 20 -> 20))
      val topKCMS = topK.taPlusIa.ta.aggregates(1).asInstanceOf[TopKCMS[Int]]
      assert(topKCMS.topKActual === 4)
      assert(topKCMS.topKInternal === 8)
      val topSetKeysIter = topKCMS.topkSet.iterator()
      (10 to 3) foreach { j =>
        val (key, value) = topSetKeysIter.next()
        assert(key === j)
        assert(value === j)
      }
    }

    {
      //add 3rd interval with next 10 keys
      topK.addEpochData(Map[Int, Long](21 -> 21, 22 -> 22, 23 -> 23, 24 -> 24, 25 -> 25,
        26 -> 26, 27 -> 27, 28 -> 28, 29 -> 29, 30 -> 30))
      val topKCMS = topK.taPlusIa.ta.aggregates(1).asInstanceOf[TopKCMS[Int]]
      assert(topKCMS.topKActual === 4)
      assert(topKCMS.topKInternal === 8)
      val topSetKeysIter = topKCMS.topkSet.iterator()
      (20 to 13) foreach { j =>
        val (key, value) = topSetKeysIter.next()
        assert(key === j)
        assert(value === j)
      }

      val topKCMS1 = topK.taPlusIa.ta.aggregates(2).asInstanceOf[TopKCMS[Int]]
      assert(topKCMS1.topKActual === 4)
      assert(topKCMS1.topKInternal === 16)
      val topSetKeysIter1 = topKCMS1.topkSet.iterator()
      (20 to 5) foreach { j =>
        val (key, value) = topSetKeysIter1.next()
        assert(key === j)
        assert(value === j)
      }

    }

  }

  //////////////////////////////////////////////////////////////////////////////
  "TopK expansion with increased time intervals in generic manner " should "be correct" in {
    val cmsParams = CMSParams(NumberUtils.nearestPowerOf2GE(8192), 7, SEED)
    val topKCount = 4
    val topK = new TopKHokusai[Int](cmsParams, 10, 0, topKCount)
    val numIntervals = 1000
    val keyWidth = 10

    {
      1 to numIntervals foreach { i =>
        var map = Map[Int, Long]()
        0 until keyWidth foreach { j =>
          map += ((i * 10 - j) -> (i * 10 - j)) // this will add like 1->1, 2->2,.... 21->21, 22->22,... 30-> 30.
        }
        topK.addEpochData(map)
        if (i == 0) {
          val topKCMS = topK.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[Int]]
          assert(topKCMS.topKActual === topKCount)
          assert(topKCMS.topKInternal === topKCount * 2)
        } else {
          val topKCMS = topK.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[Int]]
          assert(topKCMS.topKActual === topKCount)
          assert(topKCMS.topKInternal === topKCount * 2)
          val size = topK.taPlusIa.ta.aggregates.size
          1 until size foreach { j =>
            val topKCMS = topK.taPlusIa.ta.aggregates(j).asInstanceOf[TopKCMS[Int]]
            assert(topKCMS.topKActual === topKCount)
            assert(topKCMS.topKInternal === topKCount * 2 * (scala.math.pow(2, j - 1)))
            val boundedWidth = topKCount * 2 * (scala.math.pow(2, j - 1))
            //assert(topKCMS.topkSet.size() === boundedWidth)
            assert(topKCMS.topkSet.getBound() === boundedWidth)
          }

        }

      }

    }

  }
  //////////////////////////////////////////////////////////////////////////////

  "TopK elements  till last n intervals  " should "be correct" in {
    //val cmsParams = CMSParams(NumberUtils.nearestPowerOf2GE(100000),12, SEED)
    val cmsParams = CMSParams(scala.math.pow(2,18).asInstanceOf[Int],12, SEED)
    val topKCount = 200
    val topK = new TopKHokusai[Int](cmsParams, 10, 0, topKCount)
    val randomKey = new scala.util.Random(41)
    val numIntervalsToAdd = 50
    val keyRange = 200000
    val numTimesToRunQueries = 50
    val errorPercent = 2
    val intervalMap = new scala.collection.mutable.HashMap[Int, Map[Int, Long]]()
    1 to numIntervalsToAdd foreach { i =>
      var map = Map[Int, Long]()
      1 to keyRange foreach { j =>
        val value = randomKey.nextInt(Integer.MAX_VALUE)
        map += (j -> value)
      }
      /*if(i == 16) {
        System.out.println("tre")
      }*/
      topK.addEpochData(map)
      intervalMap += (i -> map)
    }
    //Check till last N interval keys
    for (i <- 1 to numTimesToRunQueries) {
      var intervalTill: Int = 0
      while (intervalTill == 0) {
        intervalTill = randomKey.nextInt(numIntervalsToAdd)
      }
      /*if(i == 7) {
        System.out.print(true)
      }*/
      val totalIntervals = intervalMap.size
      val epochTill = ((totalIntervals - intervalTill) * 10) + 5
      val topKs = topK.getTopKTillTime(epochTill)
     
      val startInterval = totalIntervals - intervalTill + 1
      val dataMap = getAllKeysCount(startInterval, totalIntervals, intervalMap)
      val sortAndBound = new BoundedSortedSet[Int](topKCount, false)
      dataMap.foreach {
        case (key, value) =>
          sortAndBound.add(key, value)
      }
      assert(sortAndBound.size() == topKs.get.length)
      val iter = sortAndBound.iterator()
      /*topKs.get.foreach {
        case (key, value) =>
          val (actualKey, actualValue) = iter.next()
          if (key != actualKey) {
            System.out.println("faaaail")
             System.out.println("actual top Keys =" + sortAndBound)
             System.out.println("calculated =" + (topKs.get.map { case (key, value) => Seq(key) }).mkString(","))
     
          }
        assert(key === actualKey)
        assert ( value == actualValue)
      }*/
      val set = scala.collection.mutable.Set[Int]()
      while(iter.hasNext()) {
        set.+=(iter.next()._1)  
      }
      var keysNotFound = 0;
      topKs.get.foreach {
        case (key, value) =>         
          if (!set.exists { x => x == key }) {
             keysNotFound +=1
          }
        //assert(key === actualKey)
        //assert ( value == actualValue)
      }
      val err = (100/topKCount)*keysNotFound 
      if( err > errorPercent) {
        topKs.get.foreach {
        case (key, value) =>         
          set.-=(key)
        //assert(key === actualKey)
        //assert ( value == actualValue)
      }
        System.out.println("keys missing in calc =" + set )
        fail("error is =" + err + "%. for query till interval ="+ intervalTill)
      }else {
        System.out.println (" query till interval = "+ intervalTill + " is approx correct with err ="+ err)
      }
    }

  }

  ////////////////////////////////////////////////////////////////
  private def computeSumOfAP(a: Int, d: Int, n: Int): Int = {
    val x: Int = (n * (2 * a + (n - 1) * d)).asInstanceOf[Int]
    x / 2
  }

  private def getAllKeysCount(startInterval: Int, endInterval: Int, dataMap: Map[Int, Map[Int, Long]]): Map[Int, Long] = {
    val finalMap = scala.collection.mutable.Map[Int, Long]()
    startInterval to endInterval foreach { i =>
      val intervalMap = dataMap(i)
     /*  val sortAndBound = new TreeMap[Long,Int]()
       intervalMap.foreach {
        case (key, value) =>  sortAndBound.put(value, key)
          
      }
        System.out.println("actual top Keys =" + sortAndBound.descendingMap())*/
      intervalMap.foreach {
        case (key, value) =>
          val prevCount = finalMap.getOrElse[Long](key, 0)
          finalMap += (key -> (prevCount + value))
      }
    }
    
   /* val sortAndBound = new TreeMap[Long,Int]()
       finalMap.foreach {
        case (key, value) =>  sortAndBound.put(value, key)
          
      }
    System.out.println("actual top Keys =" + sortAndBound.descendingMap())*/
    
    finalMap
  }
}
