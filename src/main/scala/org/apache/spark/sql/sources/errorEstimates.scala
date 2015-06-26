/**
 * Aggregates and related classes for error estimates.
 */
package org.apache.spark.sql.sources

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.partial.StudentTCacher
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.types._
import org.apache.spark.util.StatCounter

case class ErrorAvg(child: Expression, confidence: Double, isDefault: Boolean)
    extends PartialAggregate with trees.UnaryNode[Expression] {

  override def nullable = true

  override def dataType = DoubleType

  private[this] val confFactor = new NormalDistribution().
      inverseCumulativeProbability(1 - (1 - confidence) / 2)

  override def toString: String = {
    if (isDefault) s"ERROR ESTIMATE AVG($child)"
    else s"ERROR ESTIMATE ($confidence) AVG($child)"
  }

  override def asPartial: SplitEvaluation = {
    val partialStats = Alias(ErrorStatsPartition(child, confidence, isDefault,
      confFactor, isSum = false), "PartialStatsAvg")()
    SplitEvaluation(
      ErrorStatsMerge(partialStats.toAttribute,
        confidence, isDefault, confFactor, isSum = false),
      partialStats :: Nil)
  }

  override def newInstance() = ErrorStatsFunction(child, this,
    confidence, confFactor, isSum = false, partial = false)
}

case class ErrorSum(child: Expression, confidence: Double, isDefault: Boolean)
    extends PartialAggregate with trees.UnaryNode[Expression] {

  override def nullable: Boolean = true

  override def dataType = DoubleType

  private[sql] var generatedRatioExpr: MapColumnToWeight = _

  private[this] val confFactor = new NormalDistribution().
      inverseCumulativeProbability(1 - (1 - confidence) / 2)

  override def toString: String = {
    if (isDefault) s"ERROR ESTIMATE SUM($child)"
    else s"ERROR ESTIMATE ($confidence) SUM($child)"
  }


  override def asPartial: SplitEvaluation = {
    val partialStats = Alias(ErrorStatsPartition(child, confidence, isDefault,
      confFactor, isSum = true), "PartialStatsSum")()
    SplitEvaluation(
      ErrorStatsMerge(child, confidence, isDefault, confFactor, isSum = true),
      partialStats :: Nil)
  }

  override def newInstance() = ErrorStatsFunction(child, this,
    confidence, confFactor, isSum = true, partial = false)
}

private[spark] case object StatCounterUDT extends UserDefinedType[StatCounter] {

  override def sqlType: StructType = {
    // types for various fields of StatCounter that will be serialized
    StructType(Seq(
      StructField("count", LongType, nullable = false),
      StructField("mean", DoubleType, nullable = false),
      StructField("nvariance", DoubleType, nullable = false),
      StructField("max", DoubleType, nullable = false),
      StructField("min", DoubleType, nullable = false)))
  }

  override def serialize(obj: Any): Row = {
    obj match {
      case s: StatCounter =>
        val row = new GenericMutableRow(5)
        row.setLong(0, s.count)
        row.setDouble(1, s.mean)
        row.setDouble(2, s.m2)
        row.setDouble(3, s.max)
        row.setDouble(4, s.min)
        row
      // due to bugs in UDT serialization (SPARK-7186)
      case row: Row => row
    }
  }

  override def deserialize(datum: Any): StatCounter = {
    datum match {
      case row: Row =>
        require(row.length == 5, "StatCounterUDT.deserialize given row " +
            s"with length ${row.length} but requires length == 5")
        val s = new StatCounter
        s.init(count = row.getLong(0), mean = row.getDouble(1),
          m = row.getDouble(2), max = row.getDouble(3), min = row.getDouble(4))
        s
      // due to bugs in UDT serialization (SPARK-7186)
      case s: StatCounter => s
    }
  }

  override def userClass = classOf[StatCounter]

  private[spark] override def asNullable = this

  def finishEvaluation(errorStats: StatCounter, confidence: Double,
      confFactor: Double, isSum: Boolean) = {
    val count = errorStats.count
    val stdev = math.sqrt(errorStats.variance / count)

    // 30 is taken to be cut-off limit in most statistics calculations
    // for z vs t distributions (unlike StudentTCacher that uses 100)
    val errorMean =
      if (count >= 30) stdev * confFactor
      // TODO: somehow cache this at the whole evaluation level
      // (wrapper LogicalPlan?)
      // the expensive t-distribution
      else stdev * new StudentTCacher(confidence).get(count)

    if (isSum) ???
    else errorMean
  }
}

case class ErrorStatsPartition(child: Expression, confidence: Double,
    isDefault: Boolean, confFactor: Double, isSum: Boolean)
    extends AggregateExpression with trees.UnaryNode[Expression] {

  override def nullable: Boolean = false

  override def dataType: DataType = StatCounterUDT

  override def toString: String = {
    if (isDefault) s"ERROR STATS($child)"
    else s"ERROR STATS($confidence)($child)"

  }
  override def newInstance() = ErrorStatsFunction(child, this,
    confidence, confFactor, isSum, partial = true)
}

case class ErrorStatsFunction(expr: Expression, base: AggregateExpression,
    confidence: Double, confFactor: Double, isSum: Boolean,
    partial: Boolean) extends AggregateFunction {

  // Required for serialization
  def this() = this(null, null, 0.0, 0.0, false, false)

  private val errorStats = new StatCounter()

  private val castExpressionType = expr.dataType match {
    case DoubleType => 0
    case x: NumericType => 1
    case DecimalType.Fixed(_, _) => 2
    case other => sys.error(s"Type $other does not support error estimation")
  }

  private val numericCast = expr.dataType match {
    case x: NumericType => x.numeric.asInstanceOf[Numeric[Any]]
    case _ => null
  }

  override def update(input: Row): Unit = {
    val result = expr.eval(input)
    if (result != null) {
      errorStats.merge(castExpressionType match {
        case 0 => result.asInstanceOf[Double]
        case 1 => numericCast.toDouble(result)
        case 2 => result.asInstanceOf[Decimal].toDouble
      })
    }
  }

  override def eval(input: Row): Any = {
    if (partial) errorStats
    else StatCounterUDT.finishEvaluation(errorStats,
      confidence, confFactor, isSum)
  }
}

case class ErrorStatsMerge(child: Expression, confidence: Double,
    isDefault: Boolean, confFactor: Double, isSum: Boolean)
    extends AggregateExpression with trees.UnaryNode[Expression] {

  override def nullable: Boolean = false

  override def dataType: DataType = DoubleType

  override def toString: String = {
    if (isDefault) s"ERROR STATS MERGE($child)"
    else s"ERROR STATS MERGE($confidence)($child)"
  }

  override def newInstance(): ErrorStatsMergeFunction =
    ErrorStatsMergeFunction(child, this, confidence, confFactor, isSum)
}

case class ErrorStatsMergeFunction(expr: Expression, base: AggregateExpression,
    confidence: Double, confFactor: Double, isSum: Boolean)
    extends AggregateFunction {

  // Required for serialization
  def this() = this(null, null, 0.0, 0.0, false)

  private val errorStats = new StatCounter()

  override def update(input: Row): Unit = {
    val result = expr.eval(input)
    errorStats.merge(result.asInstanceOf[StatCounter])
  }

  override def eval(input: Row) = StatCounterUDT.finishEvaluation(errorStats,
    confidence, confFactor, isSum)
}
