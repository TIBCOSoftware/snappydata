package org.apache.spark.sql.execution.cms

import io.snappydata.util.com.clearspring.analytics.stream.membership.Filter
import scala.util.Random
import scala.math.ceil
import scala.Array
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import scala.reflect.ClassTag
import scala.reflect.classTag
import io.snappydata.util.NumberUtils
import org.apache.spark.sql.execution.Approximate

/*
Calculating W & D

There are two parameters (values) used when calculating the correct W and D dimensions of a count min sketch, for the desired accuracy levels. The parameters are ε (epsilon) and δ (delta).

ε (Epsilon) is “how much error is added to our counts with each item we add to the cm sketch”.

δ (Delta) is “with what probability do we want to allow the count estimate to be outside of our epsilon error rate”

To calculate W and D, you use these formulas:

W = ⌈e/ε⌉     ( The ⌈ ⌉ symbol is ceiling  meaning ⌈x⌉ means the 
ceiling of x, i.e. the smallest integer greater than or equal to x.)

D = ⌈ln (1/δ)⌉

Where ln is “natural log” and e is “euler’s constant”.

Accuracy Guarantees

When querying to get a count for a specific object (also called a “point query”)
 the accuracy guarantees are:

1) True Count <= Estimated Count
2) Estimated Count <= True Count + ε * Number Of Items Added
There is a δ chance that #2 is not true
* 
*/

class CountMinSketch[T: ClassTag](val depth: Int, val width: Int, val seed: Int,
  val eps: Double, val confidence: Double, var size: Long, val table: Array[Array[Long]],
  val hashA: Array[Long]) {
  
  val isTuple = checkForTuple

  private def checkForTuple: Boolean = {
    classTag[T].runtimeClass.getName.startsWith("scala.Tuple")
  }

  def this(depth: Int, width: Int, seed: Int) = this(depth, width, seed,
    CountMinSketch.initEPS(width), CountMinSketch.initConfidence(depth), 0,
    CountMinSketch.initTable(depth, width), CountMinSketch.initHash(depth, seed))

  def this(depth: Int, width: Int, hashA: Array[Long]) = this(depth, width, 0,
    CountMinSketch.initEPS(width), CountMinSketch.initConfidence(depth), 0,
    CountMinSketch.initTable(depth, width), hashA)

  def this(depth: Int, width: Int, hashA: Array[Long], confidence: Double, eps: Double) =
    this(depth, width, 0,
    eps, confidence, 0,
    CountMinSketch.initTable(depth, width), hashA)  
    
  def this(epsOfTotalCount: Double, confidence: Double, seed: Int) =
    this(CountMinSketch.initDepth(confidence), CountMinSketch.initWidth(epsOfTotalCount),
      seed, epsOfTotalCount, confidence, 0,
      CountMinSketch.initTable(CountMinSketch.initDepth(confidence),
        CountMinSketch.initWidth(epsOfTotalCount)),
      CountMinSketch.initHash(CountMinSketch.initDepth(confidence), seed))

  def this(depth: Int, width: Int, size: Long, hashA: Array[Long], table: Array[Array[Long]])
  = this(depth, width, 0, CountMinSketch.initEPS(width),  CountMinSketch.initConfidence(depth),
    size, table, hashA)
    
  private def this(depth: Int, width: Int, size: Long, hashA: Array[Long], table: Array[Array[Long]],
      confidence: Double)
  = this(depth, width, 0, CountMinSketch.initEPS(width),  confidence,
    size, table, hashA)  

  def getRelativeError: Double = this.eps

  def getConfidence: Double = this.confidence

  private def hash(item: Long, i: Int): Int = {
    var hash = hashA(i) * item;
    // A super fast way of computing x mod 2^p-1
    // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
    // page 149, right after Proposition 7.
    hash += hash >> 32;
    hash &= CountMinSketch.PRIME_MODULUS;
    hash.asInstanceOf[Int]
    // Doing "%" after (int) conversion is ~2x faster than %'ing longs.
    //hash.asInstanceOf[Int] % width;
  }

  private def applyWidthOnHash(hash: Int): Int = hash % width

  /**
   * Returns the currrent total count for the key
   */
  def add(item: T, count: Long): Long = {
    if (count < 0) {
      // Actually for negative increments we'll need to use the median
      // instead of minimum, and accuracy will suffer somewhat.
      // Probably makes sense to add an "allow negative increments"
      // parameter to constructor.
      throw new IllegalArgumentException("Negative increments not implemented");
    }
    def matchItem(elem: Any): Long = {
      elem match {
        case s: String => addString(s, count)
        case num: Long => addNum(num, count)
        case num: Int => addNum(num, count)
        case num: Short => addNum(num, count)
        case num: Byte => addNum(num, count)
        case p: Product => if (isTuple) {
          matchItem(hash(p))
        } else {
          throw new UnsupportedOperationException("implement hash code for other types")
        }
        case _ => throw new UnsupportedOperationException("implement hash code for other types")
      }
    }

    val totalCount = matchItem(item)
    this.size += count
    totalCount
  }

  override def clone: CountMinSketch[T] = {
    val hashACopy = this.hashA.clone

    val tableCopy = Array.ofDim[Long](depth, width)
    for (d <- 0 until tableCopy.length) {
      Array.copy(this.table(d), 0, tableCopy(d), 0, this.table(d).length)
    }
    val newCMS = new CountMinSketch[T](this.depth, this.width, this.seed,
      this.eps, this.confidence, this.size, tableCopy,
      hashACopy)
    newCMS
  }

  private def hash(tuple: Product): Long =
    tuple.productIterator.aggregate[Long](0)(_ ^ _.hashCode(), _ ^ _)

  private def addNum(item: Long, count: Long): Long = {
    var totalCount = scala.Long.MaxValue
    for (i <- 0 until depth) {
      val hsh = applyWidthOnHash(hash(item, i))
      val prevCount = table(i)(hsh)
      val newTotal = count + prevCount
      table(i)(hsh) = newTotal;
      totalCount = math.min(totalCount, newTotal)
    }
    totalCount
  }

  private def addString(item: String, count: Long): Long = {
    val buckets: Array[Int] = Filter.getHashBuckets(item, depth, width, true);
    var totalCount = scala.Long.MaxValue
    for (i <- 0 until depth) {

      val prevCount = table(i)(buckets(i))
      val newTotal = count + prevCount
      table(i)(buckets(i)) = newTotal;
      totalCount = math.min(totalCount, newTotal)

    }
    totalCount
  }

  def getIHashesFor(item: T, applyWidth: Boolean): Array[Int] = {
    def getHashes(elem: Any): Array[Int] = {
      elem match {
        case s: String => Filter.getHashBuckets(s, depth, width, applyWidth);
        case num: Long =>
          val hashes = new Array[Int](depth)
          for (i <- 0 until depth) {
            val hsh = hash(num, i)
            hashes(i) = if (applyWidth) {
              applyWidthOnHash(hsh)
            } else {
              hsh
            }
          }
          hashes
        case num: Int =>
          val hashes = new Array[Int](depth)
          for (i <- 0 until depth) {
            val hsh = hash(num, i)
            hashes(i) = if (applyWidth) {
              applyWidthOnHash(hsh)
            } else {
              hsh
            }

          }
          hashes
        case num: Short =>
          val hashes = new Array[Int](depth)
          for (i <- 0 until depth) {
            val hsh = hash(num, i)
            hashes(i) = if (applyWidth) {
              applyWidthOnHash(hsh)
            } else {
              hsh
            }
          }
          hashes
        case num: Byte =>
          val hashes = new Array[Int](depth)
          for (i <- 0 until depth) {
            val hsh = hash(num, i)
            hashes(i) = if (applyWidth) {
              applyWidthOnHash(hsh)
            } else {
              hsh
            }
          }
          hashes
        case p: Product => if (isTuple) {
          val hsh = hash(p)
          val hashes = new Array[Int](depth)
          for (i <- 0 until depth) {
            val hsh1 = hash(hsh, i)
            hashes(i) = if (applyWidth) {
              applyWidthOnHash(hsh1)
            } else {
              hsh1
            }
          }
          hashes
        } else {
          throw new UnsupportedOperationException("implement hash code for other types")
        }
        case _ => throw new UnsupportedOperationException("implement hash code for other types")
      }
    }

    getHashes(item)
  }

  def getSize: Long = this.size

  /**
   * The estimate is correct within 'epsilon' * (total item count),
   * with probability 'confidence'.
   */
  def estimateCount(item: T): Long = {
    def matchItem(elem: Any): Long = {
      elem match {
        case s: String => estimateCount(s)
        case num: Long => estimateCount(num)
        case num: Int => estimateCount(num)
        case num: Short => estimateCount(num)
        case num: Byte => estimateCount(num)
        case p: Product => if (isTuple) {
          estimateCount(p)
        } else {
          throw new UnsupportedOperationException("implement hash code for other types")
        }
        case _ => throw new UnsupportedOperationException("implement hash code for other types")
      }
    }

    matchItem(item)
  }
  
  def estimateCountAsApproximate(item: T): Approximate = {
    def matchItem(elem: Any): Approximate = {
      val count =elem match {
        case s: String => estimateCount(s)
        case num: Long => estimateCount(num)
        case num: Int => estimateCount(num)
        case num: Short => estimateCount(num)
        case num: Byte => estimateCount(num)
        case p: Product => if (isTuple) {
          estimateCount(p)
        } else {
          throw new UnsupportedOperationException("implement hash code for other types")
        }
        case _ => throw new UnsupportedOperationException("implement hash code for other types")
      }
      this.wrapAsApproximate(count)
    }

    matchItem(item)
  }

  private def estimateCount(item: Long): Long = {
    var res = scala.Long.MaxValue;
    for (i <- 0 until this.depth) {
      res = Math.min(res, table(i)(applyWidthOnHash(hash(item, i))));
    }
    return res;
  }

  private def estimateCount(item: String): Long = {
    var res = scala.Long.MaxValue;
    val buckets = Filter.getHashBuckets(item, depth, width, true);
    for (i <- 0 until this.depth) {
      res = Math.min(res, table(i)(buckets(i)));
    }
    res;
  }

  private def estimateCount(item: Product): Long = {
    this.estimateCount(hash(item))
  }

  /**
   *
   * TODO Should this compress in place?
   * Returns a new CountMinSketch that has half the width of this CMS.
   * Each of the w/2 columns of the new CMS has the combined counts of the
   * ith and ith+w/2 columns of this CMS.  See the Hokusai paper for details.
   *
   * @TODO I think there is a bug: If w == 2, then it doesn't really compress? w+newWidth = 2!
   *       But then not much use of one that small?
   *
   * @return A new CountMinSketch that is the compressed version of this
   * @throws CMSCompressException if this CMS's width is not a power of two, or is too small.
   */
  @throws(classOf[CountMinSketch.CMSCompressException])
  def compress: CountMinSketch[T] = {
    // Make sure width is a power of two, and is bigger than 2, so we have room to compress
    if ((width & (width - 1)) != 0 || width < 2) {
      throw new CountMinSketch.CMSCompressException("current width is not power of two: " + this.width);
    }

    val newWidth = width / 2;
    val newTable = Array.ofDim[Long](depth, newWidth)
    for (d <- 0 until newTable.length) {
      for (w <- 0 until newTable(d).length) {
        newTable(d)(w) = table(d)(w) + table(d)(w + newWidth) // Compress!
      }
    }

    val newHashA = this.hashA.clone()

    return new CountMinSketch[T](depth, newWidth, size, newHashA, newTable, this.confidence);
  }

  // This is needed to test compress()  // ugh
  def getTable: Array[Array[Long]] = this.table
  
  def wrapAsApproximate(estimate: Long) : Approximate = {
    new Approximate(estimate - (size*eps).asInstanceOf[Long], estimate, estimate, confidence) 
  }

}

object CountMinSketch {
  val PRIME_MODULUS: Long = (1L << 31) - 1;

  def initEPS(width: Int): Double = scala.math.exp(1.0) / width

  def initConfidence(depth: Double): Double =  1 - 1 / scala.math.exp(depth)
  

  def initTable(depth: Int, width: Int): Array[Array[Long]] = Array.ofDim[Long](depth, width)

  def initHash(depth: Int, seed: Int): Array[Long] = {
    val r = new Random(seed);
    // We're using a linear hash functions
    // of the form (a*x+b) mod p.
    // a,b are chosen independently for each hash function.
    // However we can set b = 0 as all it does is shift the results
    // without compromising their uniformity or independence with
    // the other hashes.

    Array.fill[Long](depth)(r.nextInt(Int.MaxValue))
  }

   /**
   * Translates from 'confidence' to 'depth'.
   */
  @throws[IllegalArgumentException]("if delta is is not in (0, 1)")
  def initDepth(confidence: Double): Int = {
    require( 0 < confidence && confidence < 1, "confidence must lie in (0,1)")
    scala.math.ceil(scala.math.log(1.0/(1- confidence))).toInt
  }

  /**
   * Translates from `eps` to `width`.
   */
  @throws[IllegalArgumentException]("if eps is is not in (0, 1)")
  def initWidth(eps: Double): Int = {
    require(0 < eps && eps < 1, "eps must lie in (0, 1)")
    scala.math.ceil(truncatePrecisionError(scala.math.exp(1) / eps)).toInt
  }
  
  /**
   * Translates from `eps` to `width`.
   */
  @throws[IllegalArgumentException]("if eps is is not in (0, 1)")
  def initWidthOfPowerOf2(eps: Double): (Int, Double) = {
    require(0 < eps && eps < 1, "eps must lie in (0, 1)")
    val width =scala.math.ceil(truncatePrecisionError(scala.math.exp(1) / eps)).toInt
    val powerOf2Width = NumberUtils.nearestPowerOf2GE(width)
    val newEPS = initEPS(powerOf2Width)
    powerOf2Width -> newEPS
  }
  
   //Taken from alge bird   
  // Eliminates precision errors such as the following:
  //
  //   scala> val width = 39
  //   scala> scala.math.exp(1) / CMSFunctions.eps(width)
  //   res171: Double = 39.00000000000001   <<< should be 39.0
  //
  // Because of the actual types on which CMSFunctions operates (i.e. Int and Double), the maximum number of decimal
  // places should be 6.
  private def truncatePrecisionError(i: Double, decimalPlaces: Int = 6) =
    BigDecimal(i).setScale(decimalPlaces, BigDecimal.RoundingMode.HALF_UP).toDouble

  @throws(classOf[CountMinSketch.CMSMergeException])
  def basicMerge[T: ClassTag](estimators: CountMinSketch[T]*): (Int, Int, Array[Long], Array[Array[Long]], Long) = {

    if (estimators != null && estimators.length > 0) {
      val depth = estimators(0).depth
      val width = estimators(0).width
      val hashA = estimators(0).hashA.clone

      val table = Array.ofDim[Long](depth, width)
      var size: Long = 0

      for (estimator <- estimators) {
        if (estimator.depth != depth) {
          throw new CMSMergeException("Cannot merge estimators of different depth");
        }
        if (estimator.width != width) {
          throw new CMSMergeException("Cannot merge estimators of different width");
        }
        if (!estimator.hashA.sameElements(hashA)) {
          throw new CMSMergeException("Cannot merge estimators of different seed");
        }

        for (i <- 0 until table.length) {
          for (j <- 0 until table(i).length) {
            table(i)(j) += estimator.table(i)(j)
          }
        }
        size += estimator.size;
      }
      (depth, width, hashA, table, size)
      
    } else {
      null
    }

  }

  /**
   * Merges count min sketches to produce a count min sketch for their combined streams
   *
   * @param estimators
   * @return merged estimator or null if no estimators were provided
   * @throws CMSMergeException if estimators are not mergeable (same depth, width and seed)
   */
  @throws(classOf[CountMinSketch.CMSMergeException])
  def merge[T: ClassTag](estimators: CountMinSketch[T]*): CountMinSketch[T] = {
    val (depth, width, hashA, table, size) = basicMerge[T](estimators: _*)
    new CountMinSketch[T](depth, width, size, hashA, table, estimators(0).confidence)
  }

  def deserialize[T: ClassTag](data: Array[Byte]): CountMinSketch[T] = {
    val bis: ByteArrayInputStream = new ByteArrayInputStream(data);
    val s: DataInputStream = new DataInputStream(bis);
      val(size, depth, width, eps, confidence, hashA, table) = this.read(s)

      return new CountMinSketch[T](depth, width, 0, eps, confidence, size, table, hashA);
   
  }
  
  def read(s: DataInputStream): (Long, Int, Int, Double, Double, Array[Long], Array[Array[Long]]) = {
    
    try {

      val size = s.readLong();
      val depth = s.readInt();
      val width = s.readInt();
      val eps = initEPS(width);
      val confidence = initConfidence(depth)
      val hashA = Array.ofDim[Long](depth);
      val table = Array.ofDim[Long](depth, width);
      for (i <- 0 until depth) {
        hashA(i) = s.readLong();
        for (j <- 0 until width) {
          table(i)(j) = s.readLong();
        }
      }

      (size, depth, width, eps, confidence, hashA, table)
    } catch {
      case ex: IOException => throw new RuntimeException(ex)
    }
  }

  def serialize(sketch: CountMinSketch[_]): Array[Byte] = {
    val bos = new ByteArrayOutputStream();
    val s = new DataOutputStream(bos);
    this.write(sketch, s)
    return bos.toByteArray();
  }
  
  def write(sketch: CountMinSketch[_], s: DataOutputStream) {
    try {
      s.writeLong(sketch.size);
      s.writeInt(sketch.depth);
      s.writeInt(sketch.width);
      for (i <- 0 until sketch.depth) {
        s.writeLong(sketch.hashA(i));
        for (j <- 0 until sketch.width) {
          s.writeLong(sketch.table(i)(j));
        }
      }
      
    } catch {
      // Shouldn't happen
      case ex: IOException => throw new RuntimeException(ex)
    }
  }

  // @SuppressWarnings("serial")
  class CMSMergeException(message: String) extends FrequencyMergeException(message)

  class CMSCompressException(message: String) extends Exception(message)

}
