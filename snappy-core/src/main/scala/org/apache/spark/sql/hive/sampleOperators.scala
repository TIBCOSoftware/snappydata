package org.apache.spark.sql.hive

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Final, PartialMerge, Partial, AggregateExpression2}
import org.apache.spark.sql.execution.aggregate.SortBasedAggregate
import org.apache.spark.sql.execution.bootstrap.{OnlinePlannerUtil, BootStrapUtils, RandomSeed, Seed, TaggedAlias}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.columnar.{InMemoryColumnarTableScan, InMemoryRelation}
import org.apache.spark.sql.execution.{PhysicalRDD, Project, SparkPlan, UnaryNode}
import org.apache.spark.sql.hive.execution.HiveTableScan
import org.apache.spark.sql.types.{Metadata, DataType}


object  IdentifySampledRelation
    extends Rule[SparkPlan] {


  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case scan@HiveTableScan(_, r, _) =>  SampledRelation(withSeed(scan))

      case scan@InMemoryColumnarTableScan(_, _, r@InMemoryRelation(_, _, _, _, _, Some(tableName)))   =>
        SampledRelation(withSeed(scan))

      case scan@PhysicalRDD(_,_,_)   =>       SampledRelation(withSeed(scan))
    }
  }

  private[this] def withSeed(plan: SparkPlan) =
    Project(plan.output :+ TaggedAlias(Seed(), RandomSeed(), "__seed__")(), plan)


}

object AddScaleFactor extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case project@Project(projectList, child) => {
      val scaleAttrib = BootStrapUtils.getScaleAttribute(child)
      Project(projectList :+ AttributeReference(scaleAttrib.name,scaleAttrib.dataType,
        false,scaleAttrib.metadata)(scaleAttrib.exprId,scaleAttrib.qualifiers), child)

    }
    case aggregate@SortBasedAggregate(requiredChildDistributionExpressions,
    groupingExpressions,
    nonCompleteAggregateExpressions,
    nonCompleteAggregateAttributes,
    completeAggregateExpressions,
    completeAggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child: SparkPlan) if(nonCompleteAggregateExpressions.exists(_.mode match {
      case Partial => true
      case PartialMerge => true
      case Final => false
      case Complete => false
    } )) => {
      val transformer = (aggExp: AggregateExpression2) => {
        aggExp.aggregateFunction match {
          case org.apache.spark.sql.catalyst.expressions.aggregate.Sum(expression) =>
            val lhs = BootStrapUtils.getScaleAttribute(child)
           // WeightedSum(Multiply(args, mapExpr))
            val (l,r) = OnlinePlannerUtil.widenTypes(lhs,expression)
            val newExp = Multiply(r,l)
            val newFunc = org.apache.spark.sql.catalyst.expressions.aggregate.Sum(newExp)
            AggregateExpression2(newFunc, aggExp.mode, aggExp.isDistinct)
          case _ => aggExp
        }
      }
      val newNonCompleteAggExp = nonCompleteAggregateExpressions.map (transformer)
      val newCompleteAggExp = completeAggregateExpressions.map(transformer)
      var i = 0
      val newResultsExp = resultExpressions.map{named =>
        val ar =   newNonCompleteAggExp(i).aggregateFunction.cloneBufferAttributes(0)
        val newNamed = if( named.exprId ==  ar.exprId) {
          named
        }else {
          ar.withName(ar.name)
        }
        i = i +1
        newNamed
      }
      SortBasedAggregate(requiredChildDistributionExpressions,groupingExpressions,
        newNonCompleteAggExp, nonCompleteAggregateAttributes, completeAggregateExpressions,
      completeAggregateAttributes, initialInputBufferOffset, newResultsExp, child)
    }
  }
}

case class SampledRelation(child: SparkPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def doExecute(): RDD[InternalRow] =  child.execute

}