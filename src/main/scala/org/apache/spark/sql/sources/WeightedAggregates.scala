package org.apache.spark.sql.sources

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.StratifiedSample
import org.apache.spark.sql.types._

object WeightageRule extends Rule[LogicalPlan] {
  // Transform the plan to changed the aggregates to weighted aggregates.
  // The hidden column is pulled from the StratifiedSample
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case aggr: Aggregate =>
      val isStratifiedSample = aggr find {
        case a1: StratifiedSample => true
        case _ => false
      }
      val hiddenCol = isStratifiedSample match {
        case Some(stratifiedSample) =>
          stratifiedSample.asInstanceOf[StratifiedSample].output.
              find(p => {
            p.name == Utils.WEIGHTAGE_COLUMN_NAME
          }).getOrElse(throw new IllegalStateException(
            "Hidden column for ratio not found."))
        // The aggregate is not on a StratifiedSample. No transformations needed.
        case _ => return aggr
      }

      val generatedRatioExpr = new MapColumnToWeight(hiddenCol)

      aggr transformExpressions {
        // cheat code to run the query on sample table without applying weightages
        case alias@Alias(_, name) if name.startsWith("sample_") =>
          alias
        // TODO: Extractors should be used to find the difference between the aggregate
        // and weighted aggregate functions instead of the unclean isInstance function
        case alias@Alias(e, name) =>
          val expr = transformAggExprToWeighted(e, generatedRatioExpr)
          new Alias(expr, name)(alias.exprId,
            alias.qualifiers, alias.explicitMetadata)
      }
  }

  def transformAggExprToWeighted(e: Expression,
      mapExpr: MapColumnToWeight): Expression = {
    e transform {
      case aggr@Count(args) if !aggr.isInstanceOf[WeightedCount] =>
        WeightedCount(new CoalesceDisparateTypes(Seq(args, mapExpr)))
      case aggr@Sum(args) if !aggr.isInstanceOf[WeightedSum] =>
        WeightedSum(Multiply(args, mapExpr))
      case aggr@Average(args) if !aggr.isInstanceOf[WeightedAverage] =>
        WeightedAverage(Coalesce(Seq(Cast(args, DoubleType), mapExpr)))
      case e@ErrorEstimateAggregate(child, confidence, ratioExpr,
      isDefault, aggType) if e.ratioExpr == null =>
        ErrorEstimateAggregate(child, confidence, mapExpr,
          isDefault, aggType)
      // TODO: This repetition is bad. Find a better way.
      case Add(left, right) =>
        new Add(transformAggExprToWeighted(left, mapExpr),
          transformAggExprToWeighted(right, mapExpr))
      case Subtract(left, right) =>
        new Subtract(transformAggExprToWeighted(left, mapExpr),
          transformAggExprToWeighted(right, mapExpr))
      case Divide(left, right) =>
        new Divide(transformAggExprToWeighted(left, mapExpr),
          transformAggExprToWeighted(right, mapExpr))
      case Multiply(left, right) =>
        new Multiply(transformAggExprToWeighted(left, mapExpr),
          transformAggExprToWeighted(right, mapExpr))
      case Remainder(left, right) =>
        new Remainder(transformAggExprToWeighted(left, mapExpr),
          transformAggExprToWeighted(right, mapExpr))
      case Cast(left, dtype) =>
        new Cast(transformAggExprToWeighted(left, mapExpr), dtype)
      case Sqrt(left) =>
        new Sqrt(transformAggExprToWeighted(left, mapExpr))
      case Abs(left) =>
        new Abs(transformAggExprToWeighted(left, mapExpr))
      case UnaryMinus(left) =>
        new UnaryMinus(transformAggExprToWeighted(left, mapExpr))
    }
  }
}

case class CoalesceDisparateTypes(children: Seq[Expression]) extends Expression {

  override lazy val resolved = childrenResolved

  override def dataType: DataType = children.head.dataType

  override def nullable: Boolean = true

  // Coalesce is foldable if all children are foldable.
  override def foldable: Boolean = !children.exists(!_.foldable)

  override def toString: String = s"CoalesceDisparateTypes(${children.mkString(",")})"

  override def eval(input: Row): Any = {
    throw new IllegalStateException("Children of CoalesceDisparateTypes " +
        "should be evaluated by its parent based on their types")
  }
}

object WeightedCount {

  def apply(child: Expression): WeightedCount = {
    new WeightedCount(child)
  }
}

object WeightedSum {

  def apply(child: Expression): WeightedSum = {
    new WeightedSum(child)
  }
}

object WeightedAverage {

  def apply(child: Expression): WeightedAverage = {
    new WeightedAverage(child)
  }
}

case class MapColumnToWeight(child: Expression) extends UnaryExpression {

  override def dataType: DataType = DoubleType

  override def foldable: Boolean = child.foldable

  override def nullable: Boolean = true

  override def toString: String = s"MapColumnToWeight($child)"

  private[this] final val boundReference = child match {
    case b: BoundReference => b
    case _ => null
  }

  override final def eval(input: Row): Double = {
    val boundRef = boundReference
    if (boundRef != null) {
      try {
        val value = input.getLong(boundRef.ordinal)
        if (value != 0 || !input.isNullAt(boundRef.ordinal)) {
          val left = (value >> 32) & 0xffffffffL
          val right = value & 0xffffffffL

          if (left != 0) right.toDouble / left
          else 1.0
        }
        else {
          1.0
        }
      } catch {
        case NonFatal(e) => 1.0
        case t => throw t
      }
    }
    else {
      val evalE = child.eval(input)
      if (evalE != null) {
        val value = evalE.asInstanceOf[Long]
        val left = (value >> 32) & 0xffffffffL
        val right = value & 0xffffffffL

        if (left != 0) right.toDouble / left
        else 1.0
      } else {
        1.0
      }
    }
  }
}

class WeightedSum(child: Expression) extends Sum(child) with trees.UnaryNode[Expression] {

}

class WeightedCount(child: Expression) extends Count(child) with trees.UnaryNode[Expression] {

  override def toString: String = s"WeightedCount($child)"

  override def asPartial: SplitEvaluation = {
    val partialCount = Alias(new WeightedCount(child), "WeightedPartialCount")()
    SplitEvaluation(Coalesce(Seq(Sum(partialCount.toAttribute), Literal(0L))), partialCount :: Nil)
  }

  override def newInstance(): CountFunction = new WeightedCountFunction(child, this)
}

final class WeightedCountFunction(override val expr: Expression,
    override val base: AggregateExpression)
    extends CountFunction(expr, base) {
  def this() = this(null, null) // Required for serialization.

  var countDouble: Double = 0.0
  val expr0 = expr.asInstanceOf[CoalesceDisparateTypes].children.head
  val boundReference = expr0 match {
    case b: BoundReference => b
    case _ => null
  }
  val isNonNullLiteral = expr0 match {
    case l: Literal => l.value != null
    case _ => false
  }

  val expr1 = expr.asInstanceOf[CoalesceDisparateTypes].children(1).
      asInstanceOf[MapColumnToWeight]

  override def update(input: Row): Unit = {
    if (isNonNullLiteral) {
      countDouble += expr1.eval(input)
    }
    else {
      val boundRef = boundReference
      if (boundRef != null) {
        if (!input.isNullAt(boundRef.ordinal)) {
          countDouble += expr1.eval(input)
        }
      }
      else {
        if (expr0.eval(input) != null) {
          countDouble += expr1.eval(input)
        }
      }
    }
  }

  override def eval(input: Row): Any = countDouble.toLong
}

class WeightedAverage(child: Expression)
    extends Average(child) with trees.UnaryNode[Expression] {

  override def toString: String = s"WeightedAverage($child)"

  override def asPartial: SplitEvaluation = {

    val children = child.asInstanceOf[Coalesce].children
    child.dataType match {
      //TODO: Given that the child will always have the data type of double,
      // Is this block really required?
      case DecimalType.Fixed(_, _) | DecimalType.Unlimited =>
        // Turn the child to unlimited decimals for calculation, before going back to fixed
        val partialSum = Alias(Sum(Multiply(Cast(child, DecimalType.Unlimited),
          children(1))), "PartialSum")()
        val partialCount = Alias(WeightedCount(
          new CoalesceDisparateTypes(Seq(children.head, children(1)))), "PartialCount")()

        val castedSum = Cast(Sum(partialSum.toAttribute), DecimalType.Unlimited)
        val castedCount = Cast(Sum(partialCount.toAttribute), DecimalType.Unlimited)
        SplitEvaluation(
          Cast(Divide(castedSum, castedCount), dataType),
          partialCount :: partialSum :: Nil)

      case _ =>
        val partialSum = Alias(Sum(Multiply(children.head, children(1))), "PartialSum")()
        val partialCount = Alias(WeightedCount(
          new CoalesceDisparateTypes(Seq(children.head, children(1)))), "PartialCount")()

        val castedSum = Cast(Sum(partialSum.toAttribute), dataType)
        val castedCount = Cast(Sum(partialCount.toAttribute), dataType)
        SplitEvaluation(
          Divide(castedSum, castedCount),
          partialCount :: partialSum :: Nil)
    }
  }

  override def newInstance(): AverageFunction =
    throw new IllegalStateException("Average uses a combination of sum " +
        "and count and not an Average function")
}
