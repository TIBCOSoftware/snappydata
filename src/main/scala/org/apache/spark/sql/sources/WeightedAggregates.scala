package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.execution.StratifiedSample
import org.apache.spark.sql.types._
import org.apache.spark.sql.SampleDataFrame

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
            p.name == SampleDataFrame.WEIGHTAGE_COLUMN_NAME
          }).getOrElse(throw new IllegalStateException(
            "Hidden column for ratio not found."))
        // The aggregate is not on a StratifiedSample. No transformations needed.
        case _ => return aggr
      }

      val generatedRatioExpr = new MapColumnToWeight(hiddenCol)

      aggr transformExpressions {
        // cheat code to run the query on sample table without applying weightages
        case alias@Alias(_, name) if name.startsWith("cheat_sample") =>
          alias
        // TODO: Extractors should be used to find the difference between the aggregate
        // and weighted aggregate functions instead of the unclean isInstance function
        case alias@Alias(f@Count(args), name) if !f.isInstanceOf[WeightedCount] =>
          new Alias(WeightedCount(new CoalesceDisparateTypes(Seq(args, generatedRatioExpr))),
            name)(alias.exprId, alias.qualifiers, alias.explicitMetadata)
        case alias@Alias(f@Sum(args), name) if !f.isInstanceOf[WeightedSum] =>
          new Alias(WeightedSum(Multiply(args, generatedRatioExpr)), name)(alias.exprId,
            alias.qualifiers, alias.explicitMetadata)
        case alias@Alias(f@Average(args), name) if !f.isInstanceOf[WeightedAverage] =>
          //TODO: Check if the type conversion to double works for DecimalType as well.
          new Alias(WeightedAverage(
            Coalesce(Seq(Cast(args, DoubleType), generatedRatioExpr))), name)(alias.exprId,
                alias.qualifiers, alias.explicitMetadata)
        case alias@Alias(e@ErrorEstimateAggregate(child, confidence, ratioExpr,
        isDefault, aggType), name) if e.ratioExpr == null =>
          new Alias(ErrorEstimateAggregate(child, confidence, generatedRatioExpr,
            isDefault, aggType), name)(alias.exprId, alias.qualifiers,
                alias.explicitMetadata)
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

  override def eval(input: Row): Double = {
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

class WeightedCountFunction(override val expr: Expression,
    override val base: AggregateExpression)
    extends CountFunction(expr, base) {
  def this() = this(null, null) // Required for serialization.

  var countDouble: Double = 0.0
  val expr0 = expr.asInstanceOf[CoalesceDisparateTypes].children.head
  val expr1 = expr.asInstanceOf[CoalesceDisparateTypes].children(1).
      asInstanceOf[MapColumnToWeight]

  override def update(input: Row): Unit = {
    if (expr0.eval(input) != null) {
      countDouble += expr1.eval(input)
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
