package org.apache.spark.sql.execution

import org.apache.hadoop.metrics2.util.SampleQuantiles
import org.apache.spark.sql.catalyst.{SimpleCatalystConf, CatalystConf}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.{execution, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.{Subquery, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode =>  LogicalUnary}
import org.apache.spark.sql.execution.bootstrap._
import org.apache.spark.sql.hive.{AddScaleFactor, IdentifySampledRelation}


/**
 * Created by ashahid on 11/13/15.
 */
class SnappyQueryExecution (sqlContext: SQLContext, logical: LogicalPlan)
extends QueryExecution(sqlContext, logical.transformUp {DummyReplacer()
}) {

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

  private def modifyRule  =
    if(analyzedPlanHasSampleTable(this.analyzed)) {

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
           // AddScaleFactor,
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


  private def analyzedPlanHasSampleTable(analyzed : LogicalPlan) : Boolean =
    analyzed.find{
      case Subquery(_, _: StratifiedSample) => true
      case _ => false

    } match {
      case Some(x) => true
      case None => false

  }



  override lazy val analyzed: LogicalPlan = analyzer.execute(logical)
  override val analyzer : Analyzer = new DummyAnalyzer(sqlContext.analyzer, this)


  override  val prepareForExecution : RuleExecutor[SparkPlan] = modifyRule

  override lazy val withCachedData: LogicalPlan = {
    assertAnalyzed()
    cacheManager.useCachedData(analyzed.transformUp{
      case Dummy(child, _) => child
    })
  }

  override def toString: String = ""
}

private class DummyAnalyzer ( realAnalyzer: Analyzer, queryExecutor: SnappyQueryExecution)  extends Analyzer(EmptyCatalog, EmptyFunctionRegistry, new SimpleCatalystConf(true)) {
  override def checkAnalysis(analyzed: LogicalPlan) = realAnalyzer.checkAnalysis(analyzed.children(0))
  override def execute(logical: LogicalPlan) = Dummy(realAnalyzer.execute(logical), queryExecutor)
}

case class Dummy(child : LogicalPlan, queryExecutor: SnappyQueryExecution) extends LogicalUnary {
  override def output: Seq[Attribute] = child.output
  override lazy val schema = queryExecutor.executedPlan.schema
}

object DummyReplacer {
  def apply(): PartialFunction[ LogicalPlan, LogicalPlan] = {
    case Dummy(child , _) => child
  }
}