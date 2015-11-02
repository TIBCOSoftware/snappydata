/**
 * Aggregates and related classes for error estimates.
 */
package org.apache.spark.sql.sources

import org.apache.commons.math3.distribution.{NormalDistribution, TDistribution}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

case class ErrorEstimateAggregate(child: Expression, confidence: Double,
    ratioExpr: MapColumnToWeight, isDefault: Boolean,
    aggregateType: ErrorAggregate.Type)
    extends UnaryExpression with PartialAggregate1 {

  override def nullable = true

  override def dataType: DataType = DoubleType

  private[sql] final val confFactor = new NormalDistribution().
      inverseCumulativeProbability(0.5 + confidence / 2.0)

  override def asPartial: SplitEvaluation = {
    val partialStats = Alias(ErrorStatsPartition(child :: ratioExpr :: Nil,
      confidence, isDefault, confFactor, aggregateType),
      s"PartialStats$aggregateType")()
    SplitEvaluation(
      ErrorStatsMerge(partialStats.toAttribute,
        confidence, isDefault, confFactor, aggregateType),
      partialStats :: Nil)
  }

  override def newInstance() = ErrorStatsFunction(child, ratioExpr, this,
    confidence, confFactor, aggregateType, partial = false)

  override def toString: String = {
    if (isDefault) s"ERROR ESTIMATE $aggregateType($child)"
    else s"ERROR ESTIMATE ($confidence) $aggregateType($child)"
  }
}

object ErrorAggregate extends Enumeration {
  type Type = Value

  val Avg = Value("AVG")
  val Sum = Value("SUM")
}

/**
 * A class for tracking the statistics of a set of numbers (count, mean,
 * variance and weightedCount) in a numerically robust way.
 * Includes support for merging two counters.
 *
 * Taken from Spark's StatCounter implementation removing max and min.
 */
final class StatCounterWithFullCount(var weightedCount: Double)
    extends BaseGenericInternalRow with StatVarianceCounter with Serializable {

  /** No-arg constructor for serialization. */
  def this() = this(0)

  override protected def mergeDistinctCounter(other: StatVarianceCounter) {
    super.mergeDistinctCounter(other)
    other match {
      case s: StatCounterWithFullCount => weightedCount += s.weightedCount
    }
  }

  def merge(other: StatCounterWithFullCount) {
    if (other != this) {
      super.mergeDistinctCounter(other)
      weightedCount += other.weightedCount
    } else {
      merge(other.copy()) // Avoid overwriting fields in a weird order
    }
  }

  override def copy(): StatCounterWithFullCount = {
    val other = new StatCounterWithFullCount
    other.count = count
    other.mean = mean
    other.nvariance = nvariance
    other.weightedCount = weightedCount
    other
  }

  override def numFields: Int = 4

  protected override def genericGet(ordinal: Int): Any = {
    ordinal match {
      case 0 => count
      case 1 => mean
      case 2 => nvariance
      case 3 => weightedCount
    }
  }

  override def getLong(ordinal: Int): Long = {
    if (ordinal == 0) {
      count
    } else {
      throw new ClassCastException("cannot cast double to long")
    }
  }

  override def getDouble(ordinal: Int): Double = {
    ordinal match {
      case 1 => mean
      case 2 => nvariance
      case 3 => weightedCount
      case 0 => count
    }
  }

  override def toString: String = {
    "(count: %d, mean: %f, stdev: %f, weightedCount: %f)".format(count, mean,
      stdev, weightedCount)
  }
}

private[spark] case object StatCounterUDT
    extends UserDefinedType[StatCounterWithFullCount] {

  override def sqlType: StructType = {
    // types for various serialized fields of StatCounterWithFullCount
    StructType(Seq(
      StructField("count", LongType, nullable = false),
      StructField("mean", DoubleType, nullable = false),
      StructField("nvariance", DoubleType, nullable = false),
      StructField("weightedCount", DoubleType, nullable = false)))
  }

  override def serialize(obj: Any): InternalRow = {
    obj match {
      case s: StatCounterWithFullCount => s
    }
  }

  override def deserialize(datum: Any): StatCounterWithFullCount = {
    datum match {
      case s: StatCounterWithFullCount => s
      case row: InternalRow =>
        require(row.numFields == 4, "StatCounterUDT.deserialize given row " +
            s"with length ${row.numFields} but requires length == 4")
        val s = new StatCounterWithFullCount(row.getDouble(3))
        s.initStats(count = row.getLong(0), mean = row.getDouble(1),
          nvariance = row.getDouble(2))
        s
    }
  }

  override def typeName: String = "StatCounter"

  override def userClass: Class[StatCounterWithFullCount] =
    classOf[StatCounterWithFullCount]

  private[spark] override def asNullable = this

  def finalizeEvaluation(errorStats: StatCounterWithFullCount,
      confidence: Double, confFactor: Double, aggType: ErrorAggregate.Type) = {
    val sampleCount = errorStats.count.toDouble
    val populationCount = errorStats.weightedCount
    val stdev = math.sqrt((errorStats.nvariance / (sampleCount * sampleCount)) *
        ((populationCount - sampleCount) / populationCount))

    // 30 is taken to be cut-off limit in most statistics calculations
    // for z vs t distributions (unlike StudentTCacher that uses 100)
    val errorMean =
      if (sampleCount >= 30) stdev * confFactor
      // TODO: somehow cache this at the whole evaluation level
      // (wrapper LogicalPlan with StudentTCacher?)
      // the expensive t-distribution
      else stdev * new TDistribution(errorStats.count - 1)
          .inverseCumulativeProbability(0.5 + confidence / 2.0)

    aggType match {
      case ErrorAggregate.Avg => errorMean
      case ErrorAggregate.Sum => errorMean * populationCount
    }
  }
}

case class ErrorStatsPartition(children: Seq[Expression], confidence: Double,
    isDefault: Boolean, confFactor: Double, aggType: ErrorAggregate.Type)
    extends Expression with AggregateExpression1 {

  override def nullable: Boolean = false

  override def dataType: DataType = StatCounterUDT

  override def toString: String = {
    if (isDefault) s"ERROR STATS(${children.head})"
    else s"ERROR STATS($confidence)(${children.head})"
  }

  override def newInstance() = ErrorStatsFunction(children.head,
    children(1).asInstanceOf[MapColumnToWeight], this,
    confidence, confFactor, aggType, partial = true)
}

case class ErrorStatsFunction(expr: Expression, ratioExpr: MapColumnToWeight,
    base: AggregateExpression1, confidence: Double, confFactor: Double,
    aggType: ErrorAggregate.Type, partial: Boolean)
    extends AggregateFunction1 with CastDouble {

  // Required for serialization
  def this() = this(null, null, null, 0, 0, null, false)

  private[this] final val errorStats = new StatCounterWithFullCount()

  override val doubleColumnType: DataType = expr.dataType

  private[this] final val boundReference = expr match {
    case b: BoundReference => b
    case _ => null
  }

  init()

  override def update(input: InternalRow): Unit = {
    val boundRef = boundReference
    if (boundRef != null) {
      val v = toDouble(input, boundRef.ordinal, Double.NegativeInfinity)
      if (v != Double.NegativeInfinity) {
        errorStats.merge(v)
        // update the weighted count
        errorStats.weightedCount += ratioExpr.eval(input)
      }
    } else {
      val result = expr.eval(input)
      if (result != null) {
        errorStats.merge(toDouble(result))
        // update the weighted count
        errorStats.weightedCount += ratioExpr.eval(input)
      }
    }
  }

  override def eval(input: InternalRow): Any = {
    if (partial) errorStats
    else StatCounterUDT.finalizeEvaluation(errorStats,
      confidence, confFactor, aggType)
  }
}

case class ErrorStatsMerge(child: Expression, confidence: Double,
    isDefault: Boolean, confFactor: Double, aggType: ErrorAggregate.Type)
    extends UnaryExpression with AggregateExpression1 {

  override def nullable: Boolean = false

  override def dataType: DataType = DoubleType

  override def toString: String = {
    if (isDefault) s"ERROR STATS MERGE($child)"
    else s"ERROR STATS MERGE($confidence)($child)"
  }

  override def newInstance(): ErrorStatsMergeFunction =
    ErrorStatsMergeFunction(child, this, confidence, confFactor, aggType)
}

case class ErrorStatsMergeFunction(expr: Expression,
    base: AggregateExpression1, confidence: Double, confFactor: Double,
    aggType: ErrorAggregate.Type) extends AggregateFunction1 {

  // Required for serialization
  def this() = this(null, null, 0, 0, null)

  private[this] final val errorStats = new StatCounterWithFullCount()
  private[this] final val statsTmp = new StatCounterWithFullCount()

  override def update(input: InternalRow): Unit = {
    expr.eval(input) match {
      case stats: StatCounterWithFullCount => errorStats.merge(stats)
      case r =>
        val row = r.asInstanceOf[InternalRow]
        statsTmp.initStats(row.getLong(0), row.getDouble(1), row.getDouble(2))
        statsTmp.weightedCount = row.getDouble(3)
        errorStats.merge(statsTmp)
    }
  }

  override def eval(input: InternalRow) = StatCounterUDT.finalizeEvaluation(
    errorStats, confidence, confFactor, aggType)
}
