package org.apache.spark.sql.execution

import org.apache.hadoop.metrics2.util.SampleQuantiles
import org.apache.spark.sql.{execution, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.{Subquery, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

import org.apache.spark.sql.execution.bootstrap._
import org.apache.spark.sql.hive.{AddScaleFactor, IdentifySampledRelation}


/**
 * Created by ashahid on 11/13/15.
 */
class SnappyQueryExecution (sqlContext: SQLContext, logical: LogicalPlan)
extends QueryExecution(sqlContext, logical) {

  //private  var hasSampleTable: Boolean = false
  override  val prepareForExecution : RuleExecutor[SparkPlan] = modifyRule
 // override lazy val analyzed: LogicalPlan = modifyPlanConditionally
 /*
  private def checkForSampleTable(plan : LogicalPlan) : (Boolean, LogicalPlan) = {
    var found = false
    val modifiedPlan = plan.transformUp
    {
      case SampleTable(child) => {
        found = true
        child
      }

    }
    (found, modifiedPlan)
  }*/

  //override def prepareForExecution = newRules
 /* private def modifyPlanConditionally : LogicalPlan = {
    val (foundSample, newPlan) = checkForSampleTable(analyzer.execute(logical))
    hasSampleTable = foundSample
    newPlan
  }*/

  private def modifyRule  =
    if(analyzedPlanHasSampleTable) {

       new RuleExecutor[SparkPlan] {
        //val isDebug = false

        val batches =  Seq(
          Batch("Add exchange", Once, EnsureRequirements(sqlContext)),
          Batch("Add row converters", Once, EnsureRowFormats),
          Batch("Identify Sampled Relations", Once,
            // SafetyCheck,
            IdentifySampledRelation) ,
          Batch("Pre-Bootstrap Optimization", FixedPoint(100),
            PruneProjects
          ) ,
          Batch("Bootstrap", Once,
            AddScaleFactor,
            PushDownPartialAggregate,
            PushUpResample,
            PushUpSeed,
            ImplementResample,
            PropagateBootstrap,
            IdentifyUncertainTuples,
            CleanupOutputTuples,
            InsertCollect(false, .95)
          ) ,
          Batch("Post-Bootstrap Optimization", FixedPoint(100),
            PruneColumns,
            PushDownFilter,
            PruneProjects,
            OptimizeOperatorOrder,
            PruneFilters
          ) ,
          Batch("Consolidate Bootstrap & Lineage Embedding", Once,
            ConsolidateBootstrap(5),
            IdentifyLazyEvaluates,
            EmbedLineage
          ) ,
          Batch("Materialize Plan", Once,
            ImplementSort,
            // ImplementJoin(),
            ImplementProject(),
            ImplementAggregate(2),
            ImplementCollect(),
            CleanupAnalysisExpressions
          )


        )
      }

    }else {
      sqlContext.prepareForExecution
    }


  private def analyzedPlanHasSampleTable : Boolean =
    this.analyzed.find{
      case Subquery(_, _: StratifiedSample) => true
      case _ => false

    } match {
      case Some(x) => true
      case None => false

  }

}
