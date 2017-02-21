package io.snappydata.cluster

import org.apache.spark.SparkEnv
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{SnappySession, SparkSession, Row}
import org.scalatest.{FunSuite, BeforeAndAfterAll}

case class Data1(col1: Int, col2: Int, col3: Int)

class MemoryAccountingSuite extends FunSuite with BeforeAndAfterAll {

  val options = Map("PARTITION_BY" -> "col1")

  test("pr-region without partitions") {
    val spark: SparkSession = SparkSession
      .builder
      .appName("MemoryAccounting")
      .master("local[*]")
      .getOrCreate

    val struct = (new StructType())
      .add(StructField("col1", IntegerType, true))
      .add(StructField("col2", IntegerType, true))
      .add(StructField("col3", IntegerType, true))

    val snc = new SnappySession(spark.sparkContext)

    snc.createTable("t1", "row", struct, options)
    println(SparkEnv.get.memoryManager.storageMemoryUsed)
    val row = Row(1, 1, 1)
    snc.insert("t1", row)
    println(SparkEnv.get.memoryManager.storageMemoryUsed)

    println("Deleted rows " + snc.delete("t1", "col1=1"))
    println(SparkEnv.get.memoryManager.storageMemoryUsed)
  }

}
