/**
 * Aggregates and related classes for error estimates.
 */
package org.apache.spark.sql.sources

import org.apache.commons.math3.distribution.{NormalDistribution, TDistribution}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

case class ClosedFormErrorEstimate(child: Expression, confidence: Double,
    ratioExpr: MapColumnToWeight, isDefault: Boolean,
    aggregateType: ErrorAggregate.Type,
    error: Integer)
    extends UnaryExpression with PartialAggregate1 {

  override def nullable = true

  override def dataType: DataType = DoubleType

  var attr: AttributeReference = null
  child find {
    case ar: AttributeReference => attr = ar
      true
    case _ => false
  }

  private[sql] final val confFactor = new NormalDistribution().
      inverseCumulativeProbability(0.5 + confidence / 2.0)

  override def asPartial: SplitEvaluation = {
    val partialStats = Alias(ErrorStatsPartitionCF(attr :: ratioExpr ::Nil,
      confidence, isDefault, confFactor, aggregateType, error),
      s"PartialStats$aggregateType")()

    SplitEvaluation(
      ErrorStatsMergeCF(partialStats.toAttribute,
        confidence, isDefault, confFactor, aggregateType, error),
      partialStats :: Nil)
  }

  override def newInstance() = ErrorStatsFunctionCF(attr, ratioExpr, this,
    confidence, confFactor, aggregateType, partial = false, error)

  override def toString: String = {
    if (isDefault) s"ERROR ESTIMATE $aggregateType($child)"
    else s"ERROR ESTIMATE ($confidence) $aggregateType($child)"
  }
}

object ErrorAggregateCF extends Enumeration {
  type Type = Value

  val Avg = Value("AVG")
  val Sum = Value("SUM")
}

final class StatCounterWithFullCountCF(var weightedCount: Double = 0)
    extends StatVarianceCounter with Serializable {

  override protected def mergeDistinctCounter(other: StatVarianceCounter) {
    super.mergeDistinctCounter(other)
    other match {
      case s: StatCounterWithFullCountCF => weightedCount += s.weightedCount
    }
  }

  def merge(other: StatCounterWithFullCountCF) {
    if (other != this) {
      super.mergeDistinctCounter(other)
      weightedCount += other.weightedCount
    } else {
      merge(other.copy()) // Avoid overwriting fields in a weird order
    }
  }

  override def copy(): StatCounterWithFullCountCF = {
    val other = new StatCounterWithFullCountCF
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

private[spark] case object StatCounterUDTCF
    extends UserDefinedType[StatCounterWithFullCountCF] {

  override def sqlType: StructType = {
    // types for various serialized fields of StatCounterWithFullCountCF
    StructType(Seq(
      StructField("count", LongType, nullable = false),
      StructField("mean", DoubleType, nullable = false),
      StructField("nvariance", DoubleType, nullable = false),
      StructField("weightedCount", DoubleType, nullable = false)))
  }

  override def serialize(obj: Any): Row = {
    obj match {
      case s: StatCounterWithFullCountCF =>
        val row = new Array[Any](4)
        row(0) = s.count
        row(1) = s.mean
        row(2) = s.nvariance
        row(3) = s.weightedCount
        new GenericRow(row)
      // due to bugs in UDT serialization (SPARK-7186)
      case row: Row => row
    }
  }

  override def deserialize(datum: Any): StatCounterWithFullCountCF = {
    datum match {
      case row: Row =>
        require(row.length == 4, "StatCounterUDTCF.deserialize given row " +
            s"with length ${row.length} but requires length == 4")
        val s = new StatCounterWithFullCountCF(row.getDouble(3))
        s.initStats(count = row.getLong(0), mean = row.getDouble(1),
          nvariance = row.getDouble(2))
        s
      // due to bugs in UDT serialization (SPARK-7186)
      case s: StatCounterWithFullCountCF => s
    }
  }

  override def userClass: Class[StatCounterWithFullCountCF] = classOf[StatCounterWithFullCountCF]

  private[spark] override def asNullable = this

  def finalizeEvaluation(errorStats: StatCounterWithFullCountCF,
      confidence: Double, confFactor: Double, aggType: ErrorAggregate.Type, error: Integer): Double = {
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
      case ErrorAggregate.Avg =>
        val rel_err = math.abs(errorMean/errorStats.mean)*100
        //println(s"RelErr $rel_err")
        if (rel_err > error){
          println(s"Error percent $rel_err is greater than error percent $error specified ")
          //throw new SpecificationNotMeetException(s"Error percent $act_err is greater than error percent $error specified ")
        }
        errorStats.mean //Return sample mean

      case ErrorAggregate.Sum =>
        val rel_err = math.abs(errorMean/errorStats.mean)*100
        //println(s"RelErr $rel_err")
        if (rel_err > error){
          println(s"Error percent $rel_err is greater than error percent $error specified ")
          //throw new SpecificationNotMeetException(s"Error percent $act_err is greater than error percent $error specified ")
        }
        errorStats.mean * populationCount
    }
  }
}

case class ErrorStatsPartitionCF(children: Seq[Expression], confidence: Double,
    isDefault: Boolean, confFactor: Double, aggType: ErrorAggregate.Type, error: Integer)
    extends Expression with AggregateExpression1 {

  override def nullable: Boolean = false

  override def dataType: DataType = StatCounterUDTCF

  override def toString: String = {
    if (isDefault) s"ERROR STATS(${children.head})"
    else s"ERROR STATS($confidence)(${children.head})"
  }

  override def newInstance() = ErrorStatsFunctionCF(children.head,
    children(1).asInstanceOf[MapColumnToWeight], this,
    confidence, confFactor, aggType, partial = true, error)
}

case class ErrorStatsFunctionCF(expr: Expression, ratioExpr: MapColumnToWeight,
    base: AggregateExpression1, confidence: Double, confFactor: Double,
    aggType: ErrorAggregate.Type, partial: Boolean, error: Integer)
    extends AggregateFunction1 with CastDouble {

  // Required for serialization
  def this() = this(null, null, null, 0, 0, null, false, 0)

  private[this] final val errorStats = new StatCounterWithFullCountCF()

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
    else StatCounterUDTCF.finalizeEvaluation(errorStats,
      confidence, confFactor, aggType, error)
  }
}

case class ErrorStatsMergeCF(child: Expression, confidence: Double,
    isDefault: Boolean, confFactor: Double, aggType: ErrorAggregate.Type, error: Integer)
    extends UnaryExpression with AggregateExpression1 {

  override def nullable: Boolean = false

  override def dataType: DataType = DoubleType

  override def toString: String = {
    if (isDefault) s"ERROR STATS MERGE($child)"
    else s"ERROR STATS MERGE($confidence)($child)"
  }

  override def newInstance(): ErrorStatsMergeFunctionCF =
    ErrorStatsMergeFunctionCF(child, this, confidence, confFactor, aggType, error)
}

case class ErrorStatsMergeFunctionCF(expr: Expression,
    base: AggregateExpression1, confidence: Double, confFactor: Double,
    aggType: ErrorAggregate.Type, error: Integer) extends AggregateFunction1 {

  // Required for serialization
  def this() = this(null, null, 0, 0, null, 0)

  private val errorStats = new StatCounterWithFullCountCF()

  override def update(input: InternalRow): Unit = {
    val result = expr.eval(input)
    errorStats.merge(result.asInstanceOf[StatCounterWithFullCountCF])
  }

  override def eval(input: InternalRow) = StatCounterUDTCF.finalizeEvaluation(
    errorStats, confidence, confFactor, aggType, error)
}
/**
 * The exception thrown when error percent is more than specified.
 */
class SpecificationNotMeetException(message: String) extends RuntimeException(message)
