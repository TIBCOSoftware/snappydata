package io.snappydata.core

import org.apache.spark.sql.collection.BoundedSortedSet
import org.apache.spark.sql.execution.{TopKHokusai, CMSParams}

import org.scalatest._
import Inspectors._
import scala.collection.Map
import scala.collection.Set
import io.snappydata.util.NumberUtils
import org.apache.spark.sql.execution.cms.TopKCMS


/**
 * Spec for TopK CMS
 */
class TopKCMSSpec extends FlatSpec with Matchers {
  val SEED = 123 // Do NOT use 1 for a Seed: I think makes streamlib CMS hashing degenerate!
  val MAX_J = 8 // Number of m(j) we are keeping during tests (numSketches)
  val MAX_TIME = math.pow(2, MAX_J).asInstanceOf[Int]
  
  val FIVE_ONES = Array(1L, 1L, 1L, 1L, 1L) // data we count at each epoch
  val TEST_ACCURATE_INTERVALS = 8888
  val TEST_APRROXIMATE_INTERVALS_1 = 500
  val TEST_APRROXIMATE_INTERVALS_2 = 20
  val TEST_NUM_KEYS = 100000

  /////////////////////////////////////////////////////////////////////////////
  "TopK data in TopKCMS " should "be correct" in {
    val cmsParams = CMSParams(NumberUtils.nearestPowerOf2GE(400000), 7, SEED)
    val topK = new TopKHokusai[String](cmsParams, 1, 0, 12)
    val numKeysToAdd = 10000
    val randomCount = new scala.util.Random(41)
    var map = Map[String, Long]()
    val expectedData = new java.util.TreeMap[Int, String]()
    var uniqueValues = Set[Int]()
    for (i <- 1 to numKeysToAdd) {
      
      var value: Int = -1
      var keepGoing = true;
      while(keepGoing) {
       value = randomCount.nextInt(scala.Int.MaxValue)
       if(!uniqueValues.contains(value)) {
         keepGoing = false;
         uniqueValues = uniqueValues+(value)
       }
      }
       
      map += ((i+"") -> value)
      expectedData.put(value, (i+""))
    }
    topK.addEpochData(map)
    val topKData = topK.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[String]]
   // System.out.println("reverse data ="+ expectedData.descendingMap())
    val expectedIter = expectedData.descendingMap().entrySet().iterator()
    val boundedSortedData = new BoundedSortedSet[String](12, false)
    val iter1 = topKData.topkSet.iterator()
    while(iter1.hasNext()) {
      boundedSortedData.add(iter1.next())
    }
    val actualIter = boundedSortedData.iterator();
    while(actualIter.hasNext()) {
      val boundedKey = actualIter.next()
      val entry = expectedIter.next()
      val expectedKey = entry.getValue
      val expectedValue = entry.getKey
      assert(expectedKey === boundedKey._1)
      assert(expectedValue === boundedKey._2)
    }

  }
 /////////////////////////////////////////////////////////////////
 /////////////////////////////////////////////////////////////////////////////
  "TopK data in TopKCMS " should "be correct on adding same key with different counts" in {
    val cmsParams = CMSParams(NumberUtils.nearestPowerOf2GE(400000), 7, SEED)
    val topK = new TopKHokusai[String](cmsParams, 1, 0, 12)
    val topkCMS = topK.mBar.asInstanceOf[TopKCMS[String]]
    //Add key 1 with count 5
    topkCMS.add("1", 5)
   //Add another key 1 with count 10
    topkCMS.add("1", 10)
    assert(topkCMS.topkSet.size() === 1)
    assert(topkCMS.getFromTopKMap("1").get === 15)    

  }
  
  
  ////////////////////////////////////////////////////////////////
  private def computeSumOfAP(a: Int, d: Int, n: Int): Int = {
    val x: Int = (n * (2 * a + (n - 1) * d)).asInstanceOf[Int]
    x / 2
  }
}
