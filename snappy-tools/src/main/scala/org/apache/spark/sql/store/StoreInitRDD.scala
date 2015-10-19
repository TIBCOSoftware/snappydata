package org.apache.spark.sql.store

import java.util.Properties

import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.ExecutorLocalPartition
import org.apache.spark.sql.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcUtils, DriverRegistry}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.sources.JdbcExtendedDialect
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Created by rishim on 18/10/15.
 */
class StoreInitRDD(sc : SparkContext, url : String,  val connProperties: Properties) extends  RDD[InternalRow](sc,Nil){

  val snc = SnappyContext(sc)
  val driver = DriverRegistry.getDriverClassName(url)
  val isLoner = snc.isLoner
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    GemFireXDDialect.init()
    DriverRegistry.register(driver)
    JdbcDialects.get(url) match {
      case d: JdbcExtendedDialect =>
        val extraProps = d.extraCreateTableProperties(isLoner).propertyNames
        while (extraProps.hasMoreElements) {
          val p = extraProps.nextElement()
          if (connProperties.get(p) != null) {
            sys.error(s"Master specific property $p " +
                "shouldn't exist here in Executors")
          }
        }
    }
    val conn = JdbcUtils.createConnection(url, connProperties)
    conn.close()
    Iterator.empty
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getPartitions: Array[Partition] = {
    //TODO : Find a cleaner way of starting all executors.
    val partitions = new Array[Partition](100)
    for (p <- 0 until 100) {
      partitions(p) = new ExecutorLocalPartition(p, null)
    }
    partitions
  }

}
