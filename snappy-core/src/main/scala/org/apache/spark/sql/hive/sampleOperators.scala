package org.apache.spark.sql.hive

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.bootstrap.{RandomSeed, Seed, TaggedAlias}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.columnar.{InMemoryColumnarTableScan, InMemoryRelation}
import org.apache.spark.sql.execution.{PhysicalRDD, Project, SparkPlan, UnaryNode}
import org.apache.spark.sql.hive.execution.HiveTableScan


object  IdentifySampledRelation
    extends Rule[SparkPlan] {


  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case scan@HiveTableScan(_, r, _) =>


        SampledRelation(withSeed(scan))

      case scan@InMemoryColumnarTableScan(_, _, r@InMemoryRelation(_, _, _, _, _, Some(tableName)))
       =>

        SampledRelation(withSeed(scan))
      case scan@PhysicalRDD(_,_,_)
      =>

        SampledRelation(withSeed(scan))
    }
  }

  private[this] def withSeed(plan: SparkPlan) =
    Project(plan.output :+ TaggedAlias(Seed(), RandomSeed(), "__seed__")(), plan)


}
case class SampledRelation(child: SparkPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def doExecute(): RDD[InternalRow] =  child.execute

}