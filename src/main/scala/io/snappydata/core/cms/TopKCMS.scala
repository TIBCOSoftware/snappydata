package io.snappydata.core.cms

import scala.reflect.ClassTag
import io.snappydata.util.BoundedSortedSet

class TopKCMS[T: ClassTag](val topK: Int, depth: Int, width: Int, seed: Int,
  eps: Double, confidence: Double, size: Long, table: Array[Array[Long]],
  hashA: Array[Long]) extends CountMinSketch[T](depth, width, seed,
  eps, confidence, size, table, hashA) {

  val topkSet: BoundedSortedSet[T] = new BoundedSortedSet[T](topK)

  def this(topK: Int, depth: Int, width: Int, seed: Int) = this(topK, depth, width, seed,
    CountMinSketch.initEPS(width), CountMinSketch.initConfidence(depth), 0,
    CountMinSketch.initTable(depth, width), CountMinSketch.initHash(depth, seed))

  def this(topK: Int, depth: Int, width: Int, hashA: Array[Long]) = this(topK, depth, width, 0,
    CountMinSketch.initEPS(width), CountMinSketch.initConfidence(depth), 0,
    CountMinSketch.initTable(depth, width), hashA)

  def this(topK: Int, epsOfTotalCount: Double, confidence: Double, seed: Int) =
    this(topK, CountMinSketch.initDepth(confidence), CountMinSketch.initWidth(epsOfTotalCount),
      seed, epsOfTotalCount, confidence, 0,
      CountMinSketch.initTable(CountMinSketch.initDepth(confidence),
        CountMinSketch.initWidth(epsOfTotalCount)),
      CountMinSketch.initHash(CountMinSketch.initDepth(confidence), seed))

  def this(topK: Int, depth: Int, width: Int, size: Long, hashA: Array[Long], table: Array[Array[Long]]) = this(topK, depth, width, 0, CountMinSketch.initEPS(width), CountMinSketch.initConfidence(depth),
    size, table, hashA)

  override def add(item: T, count: Long): Long = {
    val totalCount = super.add(item, count)

    this.topkSet.add(item -> totalCount)
    totalCount
  }

  private def populateTopK(element: (T, java.lang.Long)) {
    this.topkSet.add(element)
  }

  def getFromTopKMap(key: T): Option[Long] = {
    val count = this.topkSet.get(key)
    if (count != null) {
      Some(count)
    } else {
      None
    }
  }

  def getTopK: Array[(T, Long)] = {
    val iter = this.topkSet.iterator()
    Array.fill[(T, Long)](this.topkSet.size)({
      val (key, value) = iter.next
      (key, value.longValue())

    })

  }
}

object TopKCMS {
  /**
   * Merges count min sketches to produce a count min sketch for their combined streams
   *
   * @param estimators
   * @return merged estimator or null if no estimators were provided
   * @throws CMSMergeException if estimators are not mergeable (same depth, width and seed)
   */
  @throws(classOf[CountMinSketch.CMSMergeException])
  def merge[T: ClassTag](bound: Int, estimators: Array[CountMinSketch[T]]): CountMinSketch[T] = {
    val (depth, width, hashA, table, size) = CountMinSketch.basicMerge[T](estimators: _*)
    // add all the top K keys of all the estimators
    val seqOfEstimators = estimators.toSeq
    val topkKeys = getCombinedTopKFromEstimators(seqOfEstimators,
      getUnionedTopKKeysFromEstimators(seqOfEstimators))
    val mergedTopK = new TopKCMS[T](bound, depth, width, size, hashA, table)
    topkKeys.foreach(x => mergedTopK.populateTopK(x))
    mergedTopK

  }

  def getUnionedTopKKeysFromEstimators[T](estimators: Seq[CountMinSketch[T]]): scala.collection.mutable.Set[T] = {
    val topkKeys = scala.collection.mutable.HashSet[T]()
    estimators.foreach { x =>
      val topkCMS = x.asInstanceOf[TopKCMS[T]]
      val iter = topkCMS.topkSet.iterator()
      while (iter.hasNext()) {
        topkKeys += iter.next()._1
      }
    }
    topkKeys
  }

  def getCombinedTopKFromEstimators[T](estimators: Seq[CountMinSketch[T]],
    topKKeys: scala.collection.mutable.Set[T],
    topKKeyValMap: scala.collection.mutable.Map[T, java.lang.Long] = null):
    
    scala.collection.mutable.Map[T, java.lang.Long] = {
    
    val topkKeysVals = if(topKKeyValMap == null) {
     scala.collection.mutable.HashMap[T, java.lang.Long]() 
    }else {
      topKKeyValMap
    }
    estimators.foreach { x =>
      val topkCMS = x.asInstanceOf[TopKCMS[T]]
      val iter = topkCMS.topkSet.iterator()
      var tempTopKeys = topKKeys.clone
      while (iter.hasNext()) {
        val (key, count) = iter.next()
        val prevCount = topkKeysVals.getOrElse[java.lang.Long](key, 0)
        topkKeysVals.+=(key -> (prevCount + count))
        tempTopKeys = tempTopKeys - key
      }
      tempTopKeys.foreach { key =>
        val count = x.estimateCount(key)
        val prevCount = topkKeysVals.getOrElse[java.lang.Long](key, 0)
        topkKeysVals += (key -> (prevCount + count))
      }
    }
    topkKeysVals
  }
}