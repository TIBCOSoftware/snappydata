package io.snappydata.core

import scala.io.Source
import org.scalatest._
import Inspectors._
import scala.collection.Map
import scala.collection.SortedSet
import scala.math.abs
import io.snappydata.util.NumberUtils
import io.snappydata.core.cms.TopKCMS
import io.snappydata.util.BoundedSortedSet

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

   "TopK elements  till last n intervals  " should "be correct" in {
    val cmsParams = CMSParams(NumberUtils.nearestPowerOf2GE(8192), 7, SEED)
    val topKCount = 10
    val topK = new TopKHokusai[Int](cmsParams, 10, 0, topKCount)
    val randomKey = new scala.util.Random(41)
    val numIntervalsToAdd = 20
    val keyRange = 100
    val numTimesToRunQueries = 50
    val intervalMap = new scala.collection.mutable.HashMap[Int, Map[Int, Long]]()
    1 to numIntervalsToAdd foreach {i =>
      var map = Map[Int, Long]()
      1 to keyRange foreach { j =>
        val value = randomKey.nextInt(Integer.MAX_VALUE)
        map +=(j -> value)
      }
      topK.addEpochData(map)
      intervalMap += (i-> map)
    }
    //Check till last N interval keys
    for(i <- 1 to numTimesToRunQueries) {
      var intervalTill : Int = 0        
        while( intervalTill == 0) {
         intervalTill = randomKey.nextInt(numIntervalsToAdd)         
        }
      
      val epochTill = (intervalTill * 10 ) - 5
      val topKs = topK.getTopKTillTime(epochTill)
      val totalIntervals = intervalMap.size
      val startInterval = totalIntervals - intervalTill +1
      val dataMap = getAllKeysCount(startInterval, totalIntervals, intervalMap)
      val sortAndBound = new BoundedSortedSet[Int](topKCount)
      dataMap.foreach{case(key, value) => 
        sortAndBound.add(key, value)
      }
      assert ( sortAndBound.size() == topKs.get.length)
      System.out.println("actual top Keys ="+ sortAndBound)
      System.out.println("calculated ="+ (topKs.get.map{ case(key, value)=> Seq(key)}).mkString(","))
      val iter = sortAndBound.iterator()
      topKs.get.foreach{ case(key, value) =>
        val (actualKey, actualValue) = iter.next()
        assert (key === actualKey)
        //assert ( value == actualValue)
      }
    }
 
  }
  
  ////////////////////////////////////////////////////////////////
  private def computeSumOfAP(a: Int, d: Int, n: Int): Int = {
    val x: Int = (n * (2 * a + (n - 1) * d)).asInstanceOf[Int]
    x / 2
  }
  
  private def getAllKeysCount(startInterval: Int, endInterval: Int, dataMap: Map[Int, Map[Int, Long]]) 
  : Map[Int, Long] = {
    val finalMap = scala.collection.mutable.Map[Int, Long]()
    startInterval to endInterval foreach  { i =>
      val intervalMap = dataMap(i)
      intervalMap.foreach{ case(key, value) => 
        val prevCount = finalMap.getOrElse[Long](key, 0)
        finalMap += (key -> (prevCount + value))
      }
    }
    finalMap
  }
}
