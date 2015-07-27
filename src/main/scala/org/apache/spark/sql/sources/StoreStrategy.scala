package org.apache.spark.sql.sources



import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference, Row}
import org.apache.spark.sql.catalyst.plans.logical.{Statistics, LeafNode, LogicalPlan}
import org.apache.spark.sql.execution.row.JDBCUpdatableRelation
import org.apache.spark.sql.execution.{ExecutedCommand, RunnableCommand, SparkPlan}
import org.apache.spark.sql.types.StructType

/**
 * Created by rishim on 17/7/15.
 */

object StoreStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateExternalTableUsing(tableName, userSpecifiedSchema, provider, options) =>
      ExecutedCommand(
        CreateExternalTableCmd(tableName, userSpecifiedSchema, provider, options)) :: Nil
    case InsertIntoExternalTable(name, storeRelation: ExternalUpdatableStoreRelation, insertCommand) =>
      ExecutedCommand(InsertExternalTableCmd(storeRelation, insertCommand)):: Nil
    case _ => Nil
  }
}

private[sql] case class CreateExternalTableCmd(
                                                tableName: String,
                                                userSpecifiedSchema: Option[StructType],
                                                provider: String,
                                                options: Map[String, String]) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = SnappyContext(sqlContext.sparkContext)
    val resolved = ResolvedDataSource(
      sqlContext, userSpecifiedSchema, Array.empty[String], provider, options)
    snc.registerExternalTable(
      DataFrame(sqlContext, ExternalUpdatableStoreRelation(resolved.relation.asInstanceOf[JDBCUpdatableRelation])), tableName)
    Seq.empty
  }
}

private[sql] case class InsertExternalTableCmd(storeRelation: ExternalUpdatableStoreRelation, insertCommand: String) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    storeRelation.relation.insert(insertCommand)
    Seq.empty
  }
}

case class ExternalUpdatableStoreRelation(relation: JDBCUpdatableRelation)
  extends LeafNode with MultiInstanceRelation {

  override val output: Seq[AttributeReference] = relation.schema.toAttributes

  // Logical Relations are distinct if they have different output for the sake of transformations.
  override def equals(other: Any): Boolean = other match {
    case l@ExternalUpdatableStoreRelation(otherRelation) => relation == otherRelation && output == l.output
    case _ => false
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(relation, output)
  }

  override def sameResult(otherPlan: LogicalPlan): Boolean = otherPlan match {
    case ExternalUpdatableStoreRelation(otherRelation) => relation == otherRelation
    case _ => false
  }

  @transient override lazy val statistics: Statistics = Statistics(
    sizeInBytes = BigInt(relation.sizeInBytes)
  )

  /** Used to lookup original attribute capitalization */
  val attributeMap: AttributeMap[AttributeReference] = AttributeMap(output.map(o => (o, o)))

  def newInstance(): this.type = ExternalUpdatableStoreRelation(relation).asInstanceOf[this.type]

  override def simpleString: String = s"ExternalUpdatableStoreRelation[${output.mkString(",")}] $relation"

}





