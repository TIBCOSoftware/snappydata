package io.snappydata.util

import scala.io.Source
import org.scalatest._
import Inspectors._  // picks up forAll () {}

/**
 * Created by pbm on 2015-04-24.
 */
class NumberUtilsSpec extends FlatSpec with Matchers {
  "NumberUtils" should "calculate ilog2 correctly" in {
    NumberUtils.ilog2(0) should be (0) // for our purposes, anyway...
    NumberUtils.ilog2(1) should be (0)

    NumberUtils.ilog2(2) should be (1)
    NumberUtils.ilog2(3) should be (1)

    NumberUtils.ilog2(4) should be (2)
    NumberUtils.ilog2(5) should be (2)

    NumberUtils.ilog2(8) should be (3)

    NumberUtils.ilog2(15) should be (3)
    NumberUtils.ilog2(16) should be (4)
    NumberUtils.ilog2(17) should be (4)

    NumberUtils.ilog2(255) should be (7)
    NumberUtils.ilog2(256) should be (8)
    NumberUtils.ilog2(257) should be (8)
  }

  it should "determine power of two" in {
    NumberUtils.isPowerOfTwo(0) should be(true)
    NumberUtils.isPowerOfTwo(1) should be(true)
    NumberUtils.isPowerOfTwo(2) should be(true)
    NumberUtils.isPowerOfTwo(4) should be(true)
    NumberUtils.isPowerOfTwo(8) should be(true)

    NumberUtils.isPowerOfTwo(3) should be(false)
    NumberUtils.isPowerOfTwo(5) should be(false)
    NumberUtils.isPowerOfTwo(6) should be(false)
    NumberUtils.isPowerOfTwo(7) should be(false)
    NumberUtils.isPowerOfTwo(9) should be(false)
  }
  ////////////////////////////////////////////////
  
   "NumberUtils" should "calculate power of 2 correctly" in {
    NumberUtils.isPowerOf2(1) should be(0)
    NumberUtils.isPowerOf2(2) should be(1)
    NumberUtils.isPowerOf2(4) should be(2)
    NumberUtils.isPowerOf2(8) should be(3)
    NumberUtils.isPowerOf2(16) should be(4)
    NumberUtils.isPowerOf2(32) should be(5)
    NumberUtils.isPowerOf2(64) should be(6)
  }
   
   it should "return -1 for numbers which are not power of two" in {
    NumberUtils.isPowerOf2(3) should be(-1)
    NumberUtils.isPowerOf2(6) should be(-1)
    NumberUtils.isPowerOf2(171) should be(-1)    
  } 
   
  /////////////////////////////////////////////////
   
   "NumberUtils" should "calculate number which is nearest power of 2 less than or equal to the number" in {
    NumberUtils.nearestPowerOf2LE(1) should be(1)
    NumberUtils.nearestPowerOf2LE(2) should be(2)
    NumberUtils.nearestPowerOf2LE(8) should be(8)
    NumberUtils.nearestPowerOf2LE(3) should be(2)
    NumberUtils.nearestPowerOf2LE(9) should be(8)
    NumberUtils.nearestPowerOf2LE(7) should be(4)
    NumberUtils.nearestPowerOf2LE(13) should be(8)
    NumberUtils.nearestPowerOf2LE(28) should be(16)
  }
   
  //////////////////////////////////////////////////
   
    
   "NumberUtils" should "calculate number which is nearest power of 2 greater than or equal to the number" in {
    NumberUtils.nearestPowerOf2GE(1) should be(1)
    NumberUtils.nearestPowerOf2GE(2) should be(2)
    NumberUtils.nearestPowerOf2GE(8) should be(8)
    NumberUtils.nearestPowerOf2GE(3) should be(4)
    NumberUtils.nearestPowerOf2GE(9) should be(16)
    NumberUtils.nearestPowerOf2GE(7) should be(8)
    NumberUtils.nearestPowerOf2GE(13) should be(16)
    NumberUtils.nearestPowerOf2GE(28) should be(32)
  }
}
