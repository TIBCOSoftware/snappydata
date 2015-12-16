package org.apache.spark.sql.execution


import org.apache.spark.sql.catalyst.analysis._

import org.apache.spark.sql.sources.{ClosedFormErrorBounds, WeightageRule, ReplaceWithSampleTable, ClosedFormErrorEstimateRule, ErrorAndConfidence, SampleTableQuery}

import org.apache.spark.sql.{SnappyContext}
import org.apache.spark.sql.catalyst.plans.logical.{Subquery}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.plans.logical.{ LogicalPlan}

import org.apache.spark.sql.execution.bootstrap._
import org.apache.spark.sql.hive.{IdentifySampledRelation}


/**
 * Created by ashahid on 11/13/15.
 */
class SnappyQueryExecution (sqlContext: SnappyContext, logical: LogicalPlan)
extends QueryExecution(sqlContext, logical) {



  //private  var hasSampleTable: Boolean = false

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

  private def modifyRule = this.analyzed.find {
    case SampleTableQuery(_, _, _, _, _) => true
    case _ => false
  } match {
    case Some(sampleTableNode) =>
      val debug = sqlContext.conf.getConfString(Constants.keyAQPDebug, "false").toBoolean
      if (sampleTableNode.asInstanceOf[SampleTableQuery].useBootstrap) {
        new RuleExecutor[SparkPlan] {

          val batches = Seq(
            Batch("Add exchange", Once, EnsureRequirements(sqlContext)),
            Batch("Add row converters", Once, EnsureRowFormats),
            Batch("Identify Sampled Relations", Once,
              // SafetyCheck,
              IdentifySampledRelation),
            Batch("Pre-Bootstrap Optimization", FixedPoint(100),
              PruneProjects
            ),
            Batch("Bootstrap", Once,
              // AddScaleFactor,
              PushDownPartialAggregate,
              PushUpResample,
              PushUpSeed,
              ImplementResample,
              PropagateBootstrap,
              IdentifyUncertainTuples,
              CleanupOutputTuples,
              InsertCollect(debug, sampleTableNode.asInstanceOf[SampleTableQuery].confidence / 100)
            ),
            Batch("Post-Bootstrap Optimization", FixedPoint(100),
              PruneColumns,
              PushDownFilter,
              PruneProjects,
              OptimizeOperatorOrder,
              PruneFilters
            ),
            Batch("Consolidate Bootstrap & Lineage Embedding", Once,
              ConsolidateBootstrap(sqlContext.conf.getConfString(Constants.keyNumBootStrapTrials,
                Constants.defaultNumBootStrapTrials.toString).toInt, debug),
              IdentifyLazyEvaluates,
              EmbedLineage
            ),
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

      }
      else
        sqlContext.prepareForExecution

    case None => sqlContext.prepareForExecution
  }





//  override lazy val analyzed: LogicalPlan = analyzer.execute(logical)
  override val analyzer : Analyzer = new AQPQueryAnalyzer(sqlContext, this)


  override  val prepareForExecution : RuleExecutor[SparkPlan] = modifyRule
  /*
  override lazy val withCachedData: LogicalPlan = {
    assertAnalyzed()
    cacheManager.useCachedData(analyzed.transformUp{
      case SampleTableQuery(child, _) => child
    })
  }*/

  override def toString: String = ""
}

private class AQPQueryAnalyzer ( sqlContext: SnappyContext, queryExecutor: SnappyQueryExecution)
  extends Analyzer(sqlContext.catalog, sqlContext.functionRegistry, sqlContext.conf) {

  override val extendedResolutionRules =
    ExtractPythonUDFs ::
      datasources.PreInsertCastAndRename ::
      ReplaceWithSampleTable ::
      ClosedFormErrorBounds ::
      WeightageRule ::
      ClosedFormErrorEstimateRule::
      Nil

  override val extendedCheckRules = Seq(
    datasources.PreWriteCheck(sqlContext.catalog))

  override def execute(logical: LogicalPlan) = {
    val plan = super.execute(logical)

    SnappyQueryExecution.analyzedPlanHasSampleTable(plan) match {
      case Some((error, confidence, useBootstrap, newPlan)) =>  SampleTableQuery(newPlan, queryExecutor, error,
        confidence, useBootstrap)
      case None => plan
    }
  }
}

object SnappyQueryExecution {

  def analyzedPlanHasSampleTable(analyzed: LogicalPlan): Option[(Double, Double, Boolean, LogicalPlan)] = {
    var useBootstrap: Boolean = true
    var foundSample: Boolean = false
    var error: Double = 0;
    var confidence: Double = 0;
    val modifiedPlan = analyzed.transformDown {
      case e: ErrorAndConfidence => {
        error = e.error
        foundSample = true
        confidence = e.confidence
        useBootstrap = !e.applyClosedForm
        e.child
      }
    }
    if (foundSample) {
      Some((error, confidence, useBootstrap, modifiedPlan))
    } else {
      None
    }
  }
}





/*
object DummyReplacer {
  def apply(): PartialFunction[ LogicalPlan, LogicalPlan] = {
    case SampleTableQuery(child , _) => child
  }
}*/
