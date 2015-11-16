package org.apache.spark.sql.execution.bootstrap

import scala.collection.mutable
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.hive.SampledRelation
import org.apache.spark.sql.collection.Utils

object BootStrapUtils {

  val boolTrue = Literal(true)
  val boolFalse = Literal(false)
  val byte0 = Literal(0.toByte)
  val byte1 = Literal(1.toByte)

  def getSeeds(attributes: Seq[Attribute]): Seq[Attribute] =
    attributes.collect { case seed@TaggedAttribute(_: Seed, _, _, _, _) => seed }

  /**
   * Get the first BootstrapCounts if any of this plan.
   */
  def getBtCnt(attributes: Seq[Attribute]): Option[TaggedAttribute] =
    attributes.collectFirst { case btCnt@TaggedAttribute(_: Bootstrap, _, _, _, _) => btCnt }

  /**
   * Get the first flag if any of this plan
   */
  def getFlag(attributes: Seq[Attribute]): Option[TaggedAttribute] =
    attributes.collectFirst { case flag@TaggedAttribute(Flag, _, _, _, _) => flag }

  def extractBounds(attributes: Seq[Attribute]): Map[ExprId, Seq[Attribute]] =
    attributes.collect { case a@TaggedAttribute(_: Bound, _, _, _, _) => a }
        .groupBy { case a@TaggedAttribute(b: Bound, _, _, _, _) => b.child }

  def explode(expr: Expression, map: Map[ExprId, Seq[Attribute]]): Seq[Expression] = expr match {
    case a: Attribute => if (map.contains(a.exprId)) map(a.exprId) else Seq(a)
    case e => cartesian(e.children.map(c => explode(c, map))).map(e.withNewChildren)
  }

  private[this] def cartesian(exprs: Seq[Seq[Expression]]): Seq[Seq[Expression]] = exprs match {
    case Seq() => Seq(Nil)
    case _ => for (x <- exprs.head; y <- cartesian(exprs.tail)) yield x +: y
  }

  /**
   * Whether attributes contain uncertain ones.
   */
  def containsUncertain(attributes: Traversable[Attribute]) = attributes.exists {
    case TaggedAttribute(Uncertain, _, _, _, _) => true
    case _ => false
  }

  def widenTypes(left: Expression, right: Expression): (Expression, Expression) =
    if (left.dataType != right.dataType) {
      HiveTypeCoercion.findTightestCommonTypeOfTwo(left.dataType, right.dataType)
          .map { widestType => (Cast(left, widestType), Cast(right, widestType))}
          .getOrElse((left, right))
    } else {
      (left, right)
    }

  val simplifyCast: PartialFunction[Expression, Expression] = {
    case expr@Cast(_: Literal, dataType) => Literal.create(expr.eval(EmptyRow), dataType)
    case Cast(e, dataType) if e.dataType == dataType => e
    case Cast(Cast(e, _), dataType) => if (e.dataType == dataType) e else Cast(e, dataType)
  }

  def scale(expr: Expression, factor: Expression) = {
    val (left, right) = widenTypes(expr, factor)
    Multiply(left, right)
  }

  /**
   * Whether attributes contain lazy evalutes.
   */
  def containsLazyEvaluates(attributes: Traversable[Attribute]) = attributes.exists {
    case TaggedAttribute(_: LazyAttribute, _, _, _, _) => true
    case _ => false
  }

  def toNamed(exprs: Seq[Expression], name: String = "_key"): Seq[NamedExpression] =
    exprs.map {
      case named: NamedExpression => named
      case expr => Alias(expr, name)()
    }


  def collectLineageRelay(output: Seq[Attribute]) = {
    val filterExpression = getFlag(output).get.toAttributeReference
    val broadcastExpressions = output.collect {
      case a@TaggedAttribute(_: LazyAttribute, _, _, _, _) => a.toAttributeReference
    }
    LineageRelay(filterExpression, broadcastExpressions)
  }

  def collectCacheInfo(input: Seq[Attribute], output: Seq[Attribute]): (Int, Int) = {
    def getFlagIndex(exprs: Seq[NamedExpression]) =
      exprs.indexWhere {
        case TaggedAttribute(Flag, _, _, _, _) => true
        case _ => false
      }
    (getFlagIndex(input), getFlagIndex(output))
  }

  def collectIntegrityInfo(
      exprs: Seq[Expression], slackParam: Double): Option[IntegrityInfo] = {
    import BoundType._

    def cleanupAttrs(attrs: Seq[Attribute]): Seq[Attribute] = attrs.map {
      case a: TaggedAttribute => a.toAttributeReference
      case o => o
    }

    def stdev(xs: Seq[Attribute]): Expression = {
      val (sum, n) = widenTypes(xs.reduce(Add), Literal(xs.length))
      val mean = Divide(sum, n)
      val (sum2, _) = widenTypes(xs.map(a => Multiply(a, a)).reduce(Add), n)
      val mean2 = Divide(sum2, n)

      val dataType = xs.head.dataType
      Cast(Multiply(Literal(slackParam), Sqrt(Subtract(mean2, Multiply(mean, mean)))), dataType)
          .transformUp(simplifyCast)
    }

    exprs.zipWithIndex.collect {
      case (alias@TaggedAlias(LazyAggrBound(_, _, _, schema, attribute, bound), _, _), idx) =>
        val boundAttrs = cleanupAttrs(bound.boundAttributes)
        val tight = alias.toAlias.toAttribute
        val slack = stdev(boundAttrs)
        bound.boundType match {
          case Lower =>
            (Coalesce(GreaterThanOrEqual(tight, attribute) :: boolTrue :: Nil),
                (idx, MaxOf(Subtract(tight, slack), attribute)),
                schema)
          case Upper =>
            (Coalesce(LessThanOrEqual(tight, attribute) :: boolTrue :: Nil),
                (idx, MinOf(Add(tight, slack), attribute)),
                schema)
        }
    } match {
      case Seq() => None
      case all =>
        val (checks, updates, schemas) = all.unzip3
        val (updateIndexes, updateExpressions) = updates.unzip
        Some(IntegrityInfo(
          checks.reduce(And), updateIndexes.toArray, updateExpressions, schemas.head))
    }
  }

  def exchange(mode: GrowthMode): GrowthMode = mode match {
    case GrowthMode(p, n) => GrowthMode(Growth.max(p, n), Growth.Fixed)
  }

  def getGrowthMode(plan: SparkPlan): GrowthMode = {
    def fullAggregate(child: SparkPlan) = {
      exchange(getGrowthMode(child)) match {
        case GrowthMode(p, Growth.Fixed) => GrowthMode(Growth.min(p, Growth.AlmostFixed), Growth.Fixed)
      }
    }

    def broadcastJoin(buildSide: BuildSide, left: SparkPlan, right: SparkPlan) = {
      val (buildMode, streamMode) = buildSide match {
        case BuildLeft => (getGrowthMode(left), getGrowthMode(right))
        case BuildRight => (getGrowthMode(right), getGrowthMode(left))
      }
      (buildMode, streamMode) match {
        case (GrowthMode(bp, bn), GrowthMode(sp, sn)) =>
          GrowthMode(Seq(sp, bp, bn).reduce(Growth.max), sn)
      }
    }

    plan match {
      case _: LeafNode => GrowthMode(Growth.Fixed, Growth.Fixed)
      case sample: SampledRelation =>

        GrowthMode(Growth.Fixed, Growth.Grow)
      case aggregate: Aggregate =>
        if (aggregate.partial) getGrowthMode(aggregate.child) else fullAggregate(aggregate.child)
      case aggregate: BootstrapAggregate =>
        if (aggregate.partial) getGrowthMode(aggregate.child) else fullAggregate(aggregate.child)
      case aggregate: BootstrapSortedAggregate =>
        if (aggregate.partial) getGrowthMode(aggregate.child) else fullAggregate(aggregate.child)
      case aggregate: AggregateWith2Inputs2Outputs =>
        fullAggregate(aggregate.child)
      case aggregate: AggregateWith2Inputs =>
        getGrowthMode(aggregate.child)
      case _: ShuffledHashJoin | _: LeftSemiJoinHash  =>
        val modes = plan.children.map(getGrowthMode).map(exchange)
        GrowthMode(modes.map(_.perPartition).reduce(Growth.max), Growth.Fixed)
      case join: BroadcastHashJoin =>
        broadcastJoin(join.buildSide, join.left, join.right)

      case unary: UnaryNode => getGrowthMode(unary.child)
      case _ => ???
    }
  }

  def getScaleAttribute(child: SparkPlan): Attribute = child.output.filter{
    _.name == Utils.WEIGHTAGE_COLUMN_NAME
  }(0)
}





