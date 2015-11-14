package org.apache.spark.sql.execution

import org.apache.hadoop.metrics2.util.SampleQuantiles
import org.apache.spark.sql.{execution, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor

import org.apache.spark.sql.execution.bootstrap._
import org.apache.spark.sql.hive.{AddScaleFactor, IdentifySampledRelation}
import org.apache.spark.sql.sources.SampleTable

/**
 * Created by ashahid on 11/13/15.
 */
class SnappyQueryExecution (sqlContext: SQLContext, logical: LogicalPlan)
extends QueryExecution(sqlContext, logical) {

  private  lazy val hasSampleTable: Boolean = checkForSampleTable
  override  val prepareForExecution : RuleExecutor[SparkPlan] = modifyRule

  private def checkForSampleTable : Boolean = {
    var found = false
    this.analyzed.transformUp
    {
      case SampleTable(child) => {
        found = true
        child
      }

    }
    found
  }

  //override def prepareForExecution = newRules

  private def modifyRule  =
    if(hasSampleTable) {

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



}
