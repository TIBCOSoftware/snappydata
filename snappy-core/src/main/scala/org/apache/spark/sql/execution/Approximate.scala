package org.apache.spark.sql.execution

/**
 * True count is > lower Bound & less than Max , with the given probability
 *
 */
class Approximate(val lowerBound: Long, val estimate: Long, val max: Long,
  val probabilityWithinBounds: Double) extends Comparable[Approximate] with Ordered[Approximate] with Serializable {

  def +(other: Approximate): Approximate = {    
    require(this.probabilityWithinBounds == other.probabilityWithinBounds)
    new Approximate(this.lowerBound + other.lowerBound, this.estimate + other.estimate,
      this.estimate + other.estimate, this.probabilityWithinBounds)
  }

  def -(other: Approximate): Approximate = {
    require(this.probabilityWithinBounds == other.probabilityWithinBounds)
    new Approximate(this.lowerBound - other.lowerBound, this.estimate - other.estimate,
      this.estimate - other.estimate, this.probabilityWithinBounds)
  }
  
  override def  compare(o: Approximate): Int = {
    if(this.estimate > o.estimate) {
      1
    }else if ( this.estimate == o.estimate) {
      0
    }else {
      -1
    }
  }
  
  override def toString = 
    "Estimate = " + this.estimate + ", Lower Bound = "+ this.lowerBound + ", upper bound = "+ this.max + 
    ", confidence = " + this.probabilityWithinBounds  
  
}

object Approximate {
  def zeroApproximate(confidence: Double) = new Approximate(0, 0, 0, confidence)
  
}

  
