package org.apache.spark.sql.store

import io.snappydata.core.{TestData, SnappySQLContext}


import org.apache.spark.{ShuffleDependency, HashPartitioner, OneToOneDependency, Logging}

import org.scalatest.FunSuite

/**
 * Created by rishim on 13/10/15.
 */
class StoreRDDSuite extends FunSuite with Logging {

  private val testSparkContext = SnappySQLContext.sparkContext

  test("Assert OneToOne dependency in case of PreservePartitioning") {

    val snc = org.apache.spark.sql.SnappyContext(testSparkContext)

    val rdd = testSparkContext.parallelize(1 to 1000, 113).map(i => TestData(i, i.toString))

    val dataDF = snc.createDataFrame(rdd)

    val preservePartitioning = true
    val storeRDD = StoreRDD(testSparkContext, dataDF.rdd, dataDF.schema, Option("key1"), preservePartitioning, 113)


    val d = storeRDD.getDependencies.head
    val result = d match {
      case d: OneToOneDependency[_] => "PASS"
      case _ => "FAIL"
    }

    assert(result.equals("PASS"))
  }

 test("Assert OneToOne dependency in case of non partitioned tables") {

    val snc = org.apache.spark.sql.SnappyContext(testSparkContext)

    val rdd = testSparkContext.parallelize(1 to 1000, 113).map(i => TestData(i, i.toString))

    val dataDF = snc.createDataFrame(rdd)

    val preservePartitioning = true
    val storeRDD = StoreRDD(testSparkContext, dataDF.rdd, dataDF.schema,  None, preservePartitioning, 113)
    val d = storeRDD.getDependencies.head
    val result = d match {
      case d: OneToOneDependency[_] => "PASS"
      case _ => "FAIL"
    }

    assert(result.equals("PASS"))
  }

  test("Assert ShuffleDependency dependency with dataframes constructed with RDD.DataFrames does not retain the partitioner. We need to find out better way to make it work.") {

    val snc = org.apache.spark.sql.SnappyContext(testSparkContext)

    val rdd = testSparkContext.parallelize(1 to 1000, 113).map(i => (i, TestData(i, i.toString)))

    val parRDD = rdd.partitionBy(new HashPartitioner(5))
    val dataDF = snc.createDataFrame(parRDD)


    val preservePartitioning = false
    val storeRDD = StoreRDD(testSparkContext, dataDF.rdd, dataDF.schema, Option("_1"), preservePartitioning, 5)
    val d = storeRDD.getDependencies.head
    val result = d match {
      case d: ShuffleDependency[_, _, _] => "PASS" // Should be one-to one dependency
      case _ => "FAIL"
    }

    assert(result.equals("PASS"))
  }

  test("Assert Shuffle dependency in case of different partitioner") {

    val snc = org.apache.spark.sql.SnappyContext(testSparkContext)

    val rdd = testSparkContext.parallelize(1 to 1000, 113).map(i => (i, TestData(i, i.toString)))

    val parRDD = rdd.partitionBy(new HashPartitioner(5))

    val dataDF = snc.createDataFrame(parRDD)

    val preservePartitioning = false
    val storeRDD = StoreRDD(testSparkContext, dataDF.rdd, dataDF.schema, Option("_1"), preservePartitioning, 11)
    val d = storeRDD.getDependencies.head

    val result = d match {
      case d: ShuffleDependency[_, _, _] => "PASS"
      case _ => "FAIL"
    }

    assert(result.equals("PASS"))
  }
}
