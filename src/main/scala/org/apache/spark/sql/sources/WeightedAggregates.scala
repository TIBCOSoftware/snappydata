package org.apache.spark.sql.sources


import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.{StratifiedSampler, LogicalRDD}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees
import  org.apache.spark.sql.StratifiedSample
import org.apache.spark.sql.catalyst.plans.logical.Project

object WeightageRule extends Rule[LogicalPlan] {
  // Transform the plan to changed the aggregates to weighted aggregates. The hidden column is
  // pulled from the StratifiedSample
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp  {
    case aggr: Aggregate  => {
      val stratifiedSample  = aggr find ({
        case a1: StratifiedSample => true
        case _ => false

      })
      val hiddenCol = stratifiedSample match {
        case Some(stratifiedSample) => {
          stratifiedSample.asInstanceOf[StratifiedSample].output.
            find(p => {p.name == StratifiedSampler.WEIGHTAGE_COLUMN_NAME}).
            getOrElse(throw new IllegalStateException("Hidden column for ratio not found."))
        }
        // The aggregate is not on a StratifiedSample. No transformations needed.
        case _ => return aggr
      }


      val generatedratioExpr = new MapColumnToWeight(hiddenCol)

      aggr transformExpressions {
        // TODO: Extractors should be used to find the difference between the
        // aggr and weighted aggregate functions instead of the unclean isInstance function
        case alias@Alias(f@Count(args), name) if (!f.isInstanceOf[WeightedCount]) =>
          new Alias(WeightedCount(new CoalesceDisparateTypes(Seq(args, generatedratioExpr))),
            name)(alias.exprId, alias.qualifiers, alias.explicitMetadata)
        case alias@Alias(f@Sum(args), name) if (!f.isInstanceOf[WeightedSum]) =>
          new Alias(WeightedSum(Multiply(args, generatedratioExpr)), name)(alias.exprId,
            alias.qualifiers, alias.explicitMetadata)
        case alias@Alias(f@Average(args), name) if (!f.isInstanceOf[WeightedAverage]) =>
          //TODO: Check if the type conversion to double works for DecimalType as well.
          new Alias(WeightedAverage(
            Coalesce(Seq(Cast(args, DoubleType), generatedratioExpr))), name)(alias.exprId,
              alias.qualifiers, alias.explicitMetadata)
      }
  }

  }
}

case class CoalesceDisparateTypes(children: Seq[Expression]) extends Expression {

  override lazy val resolved = childrenResolved

  override def dataType: DataType = children.head.dataType

  /** Coalesce is nullable if all of its children are nullable, or if it has no children. */
  override def nullable: Boolean = !children.exists(!_.nullable)

  // Coalesce is foldable if all children are foldable.
  override def foldable: Boolean = !children.exists(!_.foldable)

  override def toString: String = s"CoalesceDisparateTypes(${children.mkString(",")})"

  override def eval(input: Row): EvaluatedType = {
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
  type EvaluatedType = Any

  override def dataType: DataType = DoubleType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = true
  override def toString: String = s"MapColumnToWeight($child)"

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      0
    } else {
      val value = evalE.asInstanceOf[Long]
      val left = ((value >> 32) & 0xffffffffL)
      val right = (value & 0xffffffffL)

      if (left == 0) 0
      else right.toDouble/left
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

class WeightedCountFunction(override val expr: Expression, override val base: AggregateExpression)
  extends  CountFunction(expr, base) {
  def this() = this(null, null) // Required for serialization.

  var countDouble: Double = _
  val expr0 = expr.asInstanceOf[CoalesceDisparateTypes].children(0)
  val expr1 = expr.asInstanceOf[CoalesceDisparateTypes].children(1)

  override def update(input: Row): Unit = {

    val evaluatedExpr0 = expr0.eval(input)
    val evaluatedExpr1 = expr1.eval(input)
    if (evaluatedExpr0 != null) {
      if (evaluatedExpr1.isInstanceOf[Double])
        countDouble += evaluatedExpr1.asInstanceOf[Double]
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
      //TODO: Given that the child will always have the data type of double, Is this block really required?
      case DecimalType.Fixed(_, _) | DecimalType.Unlimited =>
        // Turn the child to unlimited decimals for calculation, before going back to fixed
        val partialSum = Alias(Sum(Multiply(Cast(child, DecimalType.Unlimited), children(1))), "PartialSum")()
        val partialCount = Alias(WeightedCount(
          new CoalesceDisparateTypes(Seq(children(0), children(1)))), "PartialCount")()

        val castedSum = Cast(Sum(partialSum.toAttribute), DecimalType.Unlimited)
        val castedCount = Cast(Sum(partialCount.toAttribute), DecimalType.Unlimited)
        SplitEvaluation(
          Cast(Divide(castedSum, castedCount), dataType),
          partialCount :: partialSum :: Nil)

      case _ =>
        val partialSum = Alias(Sum(Multiply(children(0), children(1))), "PartialSum")()
        val partialCount = Alias(WeightedCount(
          new CoalesceDisparateTypes(Seq(children(0), children(1)))), "PartialCount")()

        val castedSum = Cast(Sum(partialSum.toAttribute), dataType)
        val castedCount = Cast(Sum(partialCount.toAttribute), dataType)
        SplitEvaluation(
          Divide(castedSum, castedCount),
          partialCount :: partialSum :: Nil)
    }
  }

  override def newInstance(): AverageFunction  =
    throw new IllegalStateException("Average uses a combination of sum and count and not an Average function")
}
