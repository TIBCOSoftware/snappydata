package org.apache.spark.sql.sources


import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types.{FloatType, DoubleType, IntegralType}

import scala.collection.mutable
import scala.util.control.Breaks._

import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Subquery, LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{StratifiedSample}

/**
 * Created by sbhokare on 4/11/15.
 */
object ReplaceWithSampleTable extends Rule[LogicalPlan] {

  val DEFAULT_CONFIDENCE: Double = 95
  val DEFAULT_ERROR: Double = 10

  def apply(plan: LogicalPlan): LogicalPlan = {

    var errorPercent: Double = -1
    var confidence: Double = -1
    var applyClosedForm: Boolean = true

    def convertToDouble(expr: Expression): Double = expr.dataType match {
      case _: IntegralType => expr.eval().asInstanceOf[Int]
      case _: DoubleType => expr.eval().asInstanceOf[Double]
      case _: FloatType => expr.eval().asInstanceOf[Float]

    }

    def traverseFilter(se: Seq[Expression], qcs: mutable.ArrayBuffer[String]): Unit = {
      for (ep <- se) {
        if (ep.isInstanceOf[AttributeReference]) {
          qcs += ep.asInstanceOf[AttributeReference].name
          //println("traverseFilter.." + qcs)
        }
        else
          traverseFilter(ep.children, qcs)
      }
    }

     plan transformDown  {

      case ErrorPercent(expr, child) => {
        errorPercent = convertToDouble(expr)
        child
      } //TODO:Store confidence level some where for post-query triage

      case Confidence(expr, child) => {
        confidence = convertToDouble(expr)
        child
      } //TODO:Store confidence level some where for post-query triage

      case p@Subquery(name, child) if (!child.isInstanceOf[StratifiedSample] && (errorPercent != -1
        || confidence != -1 || SnappyContext.SnappySC.catalog.tables.exists{ case (nameX,planX) => (nameX.toString == name
        && (planX match {
          case StratifiedSample(_,_,_) => true
          case _ => false
        }))
      }))=> {

        if(errorPercent  == -1) {
          errorPercent = DEFAULT_ERROR
        }

        if(confidence  == -1) {
          confidence = DEFAULT_CONFIDENCE
        }

        val query_qcs = new mutable.ArrayBuffer[String]
        plan transformUp {
          case a: org.apache.spark.sql.catalyst.plans.logical.Aggregate => {
            for (ar <- a.groupingExpressions.seq) {
              query_qcs += ar.asInstanceOf[AttributeReference].name
              //println("GroupBy..." + query_qcs)
            }
            a
          }
          case f: org.apache.spark.sql.catalyst.plans.logical.Filter => {
            traverseFilter(f.condition.children, query_qcs)
            f
          }
        }

        val aqpTables = SnappyContext.SnappySC.catalog.tables.collect {
          case (sampleTableIdent, ss: StratifiedSample)
            if sampleTableIdent.table.contains(p.alias + "_") => {
            //if ss.table.equals(p.alias) => {
            (ss, sampleTableIdent.table)
          }

        }

        var aqp: (StratifiedSample, String) = null;
        var superset, subset: Seq[(StratifiedSample, String)] = Seq[(StratifiedSample,String)]()
        breakable {
          for (aqpTable: (StratifiedSample, String) <- aqpTables) {

            val table_qcs = aqpTable._1.options.get("qcs").get.toString.split(",")

            if ((query_qcs.toSet.--(table_qcs)).isEmpty) {
              if (query_qcs.size == table_qcs.size) {
                //println("table where QCS(table) == QCS(query)")
                aqp = aqpTable
                break
              }
              else {
                // println("table where QCS(table) is superset of QCS(query)")
                superset = superset.+:(aqpTable)
              }
            }
            else if ((query_qcs.toSet.--(table_qcs)).size > 0) {
              //println("table where QCS(table) is subset of QCS(query)")
              subset = subset.+:(aqpTable)
            }
          }
        }

        if (aqp == null) {
          if (superset.size > 0)
            aqp = superset(0) // Need to select one of the table based on sample size
          else if (subset.size > 0) {
            aqp = subset(0) //
          }
        }

        //println("aqpTable" + aqp)
        val newPlan = aqp match {
          case (sample, name) => ErrorAndConfidence(errorPercent, confidence, applyClosedForm, Subquery(name, sample))
          case _ => p
        }
        newPlan

      }
    }
  }
}

@transient
object ClosedFormErrorEstimateRule extends Rule[LogicalPlan] {

  var error: Double = 10.0
  var conf: Double = 95
  var applyClosedForm = false

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {

    case e: ErrorAndConfidence => {
      error = e.error
      conf = e.confidence
      applyClosedForm = e.applyClosedForm
      e
    }

    case a: org.apache.spark.sql.catalyst.plans.logical.Aggregate => {
      a transformExpressions {
        case al: Alias => {
          if (applyClosedForm && (al.child.isInstanceOf[WeightedAverage] || al.child.isInstanceOf[WeightedSum])) {
            val isStratifiedSample = a find {
              case ss: StratifiedSample => true
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
              case _ => return a
            }
            val ratioExpr = new MapColumnToWeight(hiddenCol)
            var aggType = ErrorAggregate.Sum
            if (al.child.isInstanceOf[WeightedAverage]) aggType = ErrorAggregate.Avg

            new Alias(ClosedFormErrorEstimate(al.child, conf / 100, ratioExpr, true, aggType, error.toInt), al.name)(al.exprId,
              al.qualifiers, al.explicitMetadata)
          }
          else
            al
        }
      }
    }
  }
}


case class ErrorPercent(errorPercentExpr: Expression, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class Confidence(confidenceExpr: Expression, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class ErrorAndConfidence( error: Double,  confidence: Double, applyClosedForm: Boolean, child: LogicalPlan) extends  UnaryNode {
  override def output: Seq[Attribute] = child.output
}
