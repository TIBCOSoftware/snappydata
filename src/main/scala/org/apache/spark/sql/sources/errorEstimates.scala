/**
 * Aggregates and related classes for error estimates.
 */
package org.apache.spark.sql.sources

import org.apache.commons.math3.distribution.{NormalDistribution, TDistribution}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.types._

case class ErrorEstimateAggregate(child: Expression, confidence: Double,
    ratioExpr: MapColumnToWeight, isDefault: Boolean,
    aggregateType: ErrorAggregate.Type)
    extends PartialAggregate with trees.UnaryNode[Expression] {

  override def nullable = true

  override def dataType: DataType = DoubleType

  private[sql] final val confFactor = new NormalDistribution().
      inverseCumulativeProbability(0.5 + confidence / 2.0)

  override def asPartial: SplitEvaluation = {
    this.otherCopyArgs
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

final class StatCounterWithFullCount(var weightedCount: Double = 0)
    extends StatVarianceCounter with Serializable {

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

  override def serialize(obj: Any): Row = {
    obj match {
      case s: StatCounterWithFullCount =>
        val row = new GenericMutableRow(4)
        row.setLong(0, s.count)
        row.setDouble(1, s.mean)
        row.setDouble(2, s.nvariance)
        row.setDouble(3, s.weightedCount)
        row
      // due to bugs in UDT serialization (SPARK-7186)
      case row: Row => row
    }
  }

  override def deserialize(datum: Any): StatCounterWithFullCount = {
    datum match {
      case row: Row =>
        require(row.length == 4, "StatCounterUDT.deserialize given row " +
            s"with length ${row.length} but requires length == 4")
        val s = new StatCounterWithFullCount(row.getDouble(3))
        s.initStats(count = row.getLong(0), mean = row.getDouble(1),
          nvariance = row.getDouble(2))
        s
      // due to bugs in UDT serialization (SPARK-7186)
      case s: StatCounterWithFullCount => s
    }
  }

  override def userClass = classOf[StatCounterWithFullCount]

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
    extends AggregateExpression {

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
    base: AggregateExpression, confidence: Double, confFactor: Double,
    aggType: ErrorAggregate.Type, partial: Boolean)
    extends AggregateFunction with CastDouble {

  // Required for serialization
  def this() = this(null, null, null, 0, 0, null, false)

  private[this] final val errorStats = new StatCounterWithFullCount()

  override def doubleColumnType: DataType = expr.dataType

  private[this] final val boundReference = expr match {
    case b: BoundReference => b
    case _ => null
  }

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
    extends AggregateExpression with trees.UnaryNode[Expression] {

  override def nullable: Boolean = false

  override def dataType: DataType = DoubleType

  override def toString: String = {
    if (isDefault) s"ERROR STATS MERGE($child)"
    else s"ERROR STATS MERGE($confidence)($child)"
  }

  override def newInstance(): ErrorStatsMergeFunction =
    ErrorStatsMergeFunction(child, this, confidence, confFactor, aggType)
}

case class ErrorStatsMergeFunction(expr: Expression, base: AggregateExpression,
    confidence: Double, confFactor: Double, aggType: ErrorAggregate.Type)
    extends AggregateFunction {

  // Required for serialization
  def this() = this(null, null, 0, 0, null)

  private val errorStats = new StatCounterWithFullCount()

  override def update(input: InternalRow): Unit = {
    val result = expr.eval(input)
    errorStats.merge(result.asInstanceOf[StatCounterWithFullCount])
  }

  override def eval(input: InternalRow) = StatCounterUDT.finalizeEvaluation(
    errorStats, confidence, confFactor, aggType)
}
