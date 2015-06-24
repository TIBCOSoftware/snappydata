package org.apache.spark.sql.execution.cms

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.collection.{BoundedSortedSet, SegmentMap}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable
import scala.reflect.ClassTag

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
  // TODO: Resolve the type of TopKCMS
  private final val topKMap = new mutable.HashMap[String, TopKCMS[Any]]
  private final val mapLock = new ReentrantReadWriteLock

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
  def apply(name: String): Option[TopKCMS[Any]] = {
    SegmentMap.lock(mapLock.readLock) {
      topKMap.get(name)
    }
  }

  def apply(name: String, confidence: Double,
            eps : Double, size: Int) = {
    lookupOrAdd(name, confidence, eps, size)
  }

  private[sql] def lookupOrAdd(name: String,
                               confidence: Double, eps : Double, size: Int) : TopKCMS[Any] = {
    SegmentMap.lock(mapLock.readLock) {
      topKMap.get(name)
    } match {
      case Some(topk) => topk
      case None =>
        // insert into global map but double-check after write lock
        SegmentMap.lock(mapLock.writeLock) {
          topKMap.getOrElse(name, {
            // TODO: why is seed needed as an input param
            val topk = new TopKCMS[Any](size, eps, confidence, 123)
            topKMap(name) = topk
            topk
          })
        }
    }
  }

}
class TopKWrapper[T] (val topKCMS: TopKCMS[T],
                      val schema: StructType, val key : StructField)

object TopKWrapper {


  implicit class StringExtensions(val s: String) extends AnyVal {
    def ci = new {
      def unapply(other: String) = s.equalsIgnoreCase(other)
    }
  }

  def apply(name: String , options: Map[String, Any],
            schema: StructType) : TopKWrapper[Any] = {
    val keyTest = "key".ci
    val timeIntervalTest = "timeInterval".ci
    val confidenceTest = "confidence".ci
    val epsTest = "eps".ci
    val sizeTest = "size".ci

    val cols = schema.fieldNames

    // using a default strata size of 104 since 100 is taken as the normal
    // limit for assuming a Gaussian distribution (e.g. see MeanEvaluator)
    val defaultStrataSize = 104
    // Using foldLeft to read key-value pairs and build into the result
    // tuple of (qcs, fraction, strataReservoirSize) like an aggregate.
    // This "aggregate" simply keeps the last values for the corresponding
    // keys as found when folding the map.
    val (key, timeInterval, confidence, eps, size) = options.
      foldLeft("", 5, 0.95, 0.01, 100) {
      case ((k, ti, cf, e, s), (opt, optV)) =>
        opt match {
          case keyTest() => (optV.toString, ti, cf, e, s)
          case confidenceTest() => optV match {
            case fd: Double => (k, ti, fd, e, s)
            case fs: String => (k, ti, fs.toDouble, e, s)
            case ff: Float => (k, ti, ff.toDouble, e, s)
            case fi: Int => (k, ti, fi.toDouble, e, s)
            case fl: Long => (k, ti, fl.toDouble, e, s)
            case _ => throw new AnalysisException(
              s"TopKCMS: Cannot parse double 'confidence'=$optV")
          }
          case epsTest() => optV match {
            case fd: Double => (k, ti, cf, fd, s)
            case fs: String => (k, ti, cf, fs.toDouble, s)
            case ff: Float => (k, ti, cf, ff.toDouble, s)
            case fi: Int => (k, ti, cf, fi.toDouble, s)
            case fl: Long => (k, ti, cf, fl.toDouble, s)
            case _ => throw new AnalysisException(
              s"TopKCMS: Cannot parse double 'eps'=$optV")
          }

          case timeIntervalTest() => optV match {
            case si: Int => (k, si, cf, e, s)
            case ss: String => (k, ss.toInt, cf, e, s)
            case sl: Long => (k, sl.toInt, cf, e, s)
            case _ => throw new AnalysisException(
              s"TopKCMS: Cannot parse int 'timeInterval'=$optV")
          }
          case sizeTest() => optV match {
            case si: Int => (k, ti, cf, e, si)
            case ss: String => (k, ti, cf, e, ss.toInt)
            case sl: Long => (k, ti, cf, e, sl.toInt)
            case _ => throw new AnalysisException(
              s"TopKCMS: Cannot parse int 'size'=$optV")
          }
        }
    }
    new TopKWrapper(TopKCMS.lookupOrAdd(name, confidence, eps, size),
      schema, schema(key))

  }


}