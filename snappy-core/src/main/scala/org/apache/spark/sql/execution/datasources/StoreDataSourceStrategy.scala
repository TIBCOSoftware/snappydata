package org.apache.spark.sql.execution.datasources

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.execution.PartitionedDataSourceScan
import org.apache.spark.sql.execution.datasources.DataSourceStrategy._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, Strategy, execution}

/**
 * This strategy makes a PartitionedPhysicalRDD out of a PrunedFilterScan based datasource.
 * Mostly this is a copy of DataSourceStrategy of Spark. But it takes care of the underlying
 * partitions of the datasource.
 */
private[sql] object StoreDataSourceStrategy extends Strategy with Logging {

  def apply(plan: LogicalPlan): Seq[execution.SparkPlan] = plan match {
    case PhysicalOperation(projects, filters, l@LogicalRelation(t: PartitionedDataSourceScan)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        t.numPartitions,
        t.partitionColumns,
        (a, f) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray, f))) :: Nil

    case _ => Nil
  }

  /**
   * Convert RDD of Row into RDD of InternalRow with objects in catalyst types
   */
  private[this] def toCatalystRDD(
      relation: LogicalRelation,
      output: Seq[Attribute],
      rdd: RDD[Row]): RDD[InternalRow] = {
    if (relation.relation.needConversion) {
      execution.RDDConversions.rowToRowRdd(rdd, output.map(_.dataType))
    } else {
      rdd.asInstanceOf[RDD[InternalRow]]
    }
  }

  // Based on Public API.
  protected def pruneFilterProject(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      numPartition: Int,
      partitionColumns: Seq[String],
      scanBuilder: (Seq[Attribute], Array[Filter]) => RDD[InternalRow]) = {
    pruneFilterProjectRaw(
      relation,
      projects,
      filterPredicates,
      numPartition,
      partitionColumns,
      (requestedColumns, pushedFilters) => {
        scanBuilder(requestedColumns, selectFilters(pushedFilters).toArray)
      })
  }

  // Based on Catalyst expressions.
  protected def pruneFilterProjectRaw(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      numPartition: Int,
      partitionColumns: Seq[String],
      scanBuilder: (Seq[Attribute], Seq[Expression]) => RDD[InternalRow]) = {

    val projectSet = AttributeSet(projects.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filterCondition = filterPredicates.reduceLeftOption(expressions.And)

    val pushedFilters = filterPredicates.map {
      _ transform {
        case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
      }
    }

    // Get the partition column attribute INFO from relation schema
    val partitionColumnsRef = partitionColumns.map(b =>
      {
        relation.output.filter(a => {
          val c = a.withName(b)
          c.equals(a)
        }).head
      })

    if (projects.map(_.toAttribute) == projects &&
        projectSet.size == projects.size &&
        filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val requestedColumns =
        projects.asInstanceOf[Seq[Attribute]] // Safe due to if above.
          .map(relation.attributeMap) // Match original case of attributes.

      val scan = execution.PartitionedPhysicalRDD.createFromDataSource(
        projects.map(_.toAttribute),
        numPartition,
        partitionColumnsRef,
        scanBuilder(requestedColumns, pushedFilters),
        relation.relation)
      filterCondition.map(execution.Filter(_, scan)).getOrElse(scan)
    } else {
      val requestedColumns = (projectSet ++ filterSet).map(relation.attributeMap).toSeq

      val scan = execution.PartitionedPhysicalRDD.createFromDataSource(
        requestedColumns,
        numPartition,
        partitionColumnsRef,
        scanBuilder(requestedColumns, pushedFilters),
        relation.relation)
      execution.Project(projects, filterCondition.map(execution.Filter(_, scan)).getOrElse(scan))
    }
  }

}
