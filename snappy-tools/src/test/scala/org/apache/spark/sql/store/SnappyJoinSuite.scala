package org.apache.spark.sql.store

import io.snappydata.core.{FileCleaner, RefData, TestData2, TestSqlContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.joins.LocalJoin
import org.apache.spark.sql.execution.{Exchange, PartitionedPhysicalRDD, PhysicalRDD, QueryExecution}
import org.apache.spark.sql.{SaveMode, SnappyContext}
import org.apache.spark.{Logging, SparkContext}

/**
 * Created by rishim on 6/11/15.
 */
class SnappyJoinSuite extends FunSuite with Logging  with BeforeAndAfterAll{

  var sc : SparkContext= null

  override def afterAll(): Unit = {
    sc.stop()
    FileCleaner.cleanStoreFiles()
  }

  override def beforeAll(): Unit = {
    if (sc == null) {
      sc = TestSqlContext.newSparkContext
    }
  }

  val props = Map.empty[String, String]

  test("Replicated table join with PR Table") {

    val snc = org.apache.spark.sql.SnappyContext(sc)
    val rdd = sc.parallelize((1 to 5).map(i => RefData(i, s"$i")))
    val refDf = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS RR_TABLE")

    snc.sql("CREATE TABLE RR_TABLE(OrderRef INT NOT NULL,description String) USING row options()")


    refDf.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("RR_TABLE")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE")

    val df = snc.sql("CREATE TABLE PR_TABLE(OrderId INT NOT NULL,description String, OrderRef INT)" +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderRef')")

    val dimension = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, (i%5 + 1))))

    val dimensionDf = snc.createDataFrame(dimension)
    dimensionDf.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("PR_TABLE")


    val countDf = snc.sql("select * from PR_TABLE P JOIN RR_TABLE R ON P.ORDERREF = R.ORDERREF")

    val qe = new QueryExecution(snc, countDf.logicalPlan)
    val plan = qe.executedPlan
    val lj = plan collectFirst {
      case lc : LocalJoin => lc
    }
    lj.getOrElse(sys.error(s"Can't find Local join in a 1 partitioned relation"))


    val t1 = System.currentTimeMillis()
    assert(countDf.count() === 1000) // Make sure aggregation is working with local join
    val t2 = System.currentTimeMillis()
    println("Time taken = "+ (t2-t1))

    val projectDF = snc.sql("select ORDERID, P.DESCRIPTION , R.DESCRIPTION from PR_TABLE P JOIN RR_TABLE R ON P.ORDERREF = R.ORDERREF")
    assert(projectDF.columns.length === 3)

    val sumDF = snc.sql("select SUM(ORDERID)from PR_TABLE P JOIN RR_TABLE R ON P.ORDERREF = R.ORDERREF")
    assert(sumDF.collect()(0).getLong(0) === ((1000 * 1001)/2))

  }

  test("Replicated table join with Replicated Table") {

    val snc = org.apache.spark.sql.SnappyContext(sc)
    val rdd = sc.parallelize((1 to 5).map(i => RefData(i, s"$i")))
    val refDf = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS RR_TABLE1")

    snc.sql("CREATE TABLE RR_TABLE1(OrderRef INT NOT NULL,description String) USING row options()")


    refDf.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("RR_TABLE1")

    snc.sql("DROP TABLE IF EXISTS RR_TABLE2")

    val df = snc.sql("CREATE TABLE RR_TABLE2(OrderId INT NOT NULL,description String, OrderRef INT) USING row options()" )


    val dimension = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, (i%5 + 1))))

    val dimensionDf = snc.createDataFrame(dimension)
    dimensionDf.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("RR_TABLE2")


    val countDf = snc.sql("select * from RR_TABLE1 P JOIN RR_TABLE2 R ON P.ORDERREF = R.ORDERREF")
    val qe = new QueryExecution(snc, countDf.logicalPlan)

    val lj = qe.executedPlan collectFirst {
      case lc : LocalJoin => lc
    }
    lj.getOrElse(sys.error(s"Can't find Local join in a 1 partitioned relation"))

    val t1 = System.currentTimeMillis()
    assert(countDf.count() === 1000) // Make sure aggregation is working with local join
    val t2 = System.currentTimeMillis()
    println("Time taken = "+ (t2-t1))

    val projectDF = snc.sql("select R.ORDERID, P.DESCRIPTION , R.DESCRIPTION from RR_TABLE1 P JOIN RR_TABLE2 R ON P.ORDERREF = R.ORDERREF")
    assert(projectDF.columns.length === 3)

    val sumDF = snc.sql("select SUM(R.ORDERID)from RR_TABLE1 P JOIN RR_TABLE2 R ON P.ORDERREF = R.ORDERREF")
    assert(sumDF.collect()(0).getLong(0) === ((1000 * 1001)/2))

  }

  /**
   * This method is very specific to  PartitionedDataSourceScan and snappy join improvements
   *
   */
  private def checkForShuffle(plan :LogicalPlan, snc : SnappyContext, shuffleExpected : Boolean): Unit ={

    val qe = new QueryExecution(snc, plan)
    val lj = qe.executedPlan collect {
      case ex : Exchange => ex
    }
    if(shuffleExpected){
      if(lj.length == 0) sys.error(s"Shuffle Expected , but was not found")
    }else{
      lj.foreach(a => a.child.collectFirst { // this means no Exhange should have child as PartitionedPhysicalRDD
        case p : PartitionedPhysicalRDD => sys.error(s"Did not expect exchange with partitioned scan with same partitions")
        case p : PhysicalRDD => sys.error(s"Did not expect PhyscialRDD with PartitionedDataSourceScan")
        case _ => // do nothing, may be some other Exchange and not with scan
      })
    }
  }

  test("PR table join with PR Table") {

    val snc = org.apache.spark.sql.SnappyContext(sc)
    val dimension1 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, (i%10 + 1))))
    val refDf = snc.createDataFrame(dimension1)
    snc.sql("DROP TABLE IF EXISTS PR_TABLE1")

    snc.sql("CREATE TABLE PR_TABLE1(OrderId INT NOT NULL,description String, OrderRef INT)" +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId, OrderRef')")


    refDf.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("PR_TABLE1")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE2")

    snc.sql("CREATE TABLE PR_TABLE2(OrderId INT NOT NULL,description String, OrderRef INT)" +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId,OrderRef')")

    val dimension2 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, (i%5 + 1))))

    val dimensionDf = snc.createDataFrame(dimension2)
    dimensionDf.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("PR_TABLE2")


    val excatJoinKeys = snc.sql("select P.ORDERREF, P.DESCRIPTION from PR_TABLE1 P JOIN PR_TABLE2 R ON P.ORDERID = R.ORDERID AND P.ORDERREF = R.ORDERREF")
    checkForShuffle(excatJoinKeys.logicalPlan, snc, false)

    val t1 = System.currentTimeMillis()
    assert(excatJoinKeys.count() === 500) // Make sure aggregation is working with non-shuffled joins
    val t2 = System.currentTimeMillis()
    println("Time taken = "+ (t2-t1))
    val df2 = excatJoinKeys.head(2)
    df2.foreach(println)



    // Reverse the join keys
    val reverseJoinKeys = snc.sql("select P.ORDERREF, P.DESCRIPTION from PR_TABLE1 P JOIN PR_TABLE2 R ON P.ORDERREF = R.ORDERREF AND P.ORDERID = R.ORDERID")
    checkForShuffle(reverseJoinKeys.logicalPlan, snc, false)

    // Partial join keys
    val partialJoinKeys = snc.sql("select P.ORDERREF, P.DESCRIPTION from PR_TABLE1 P JOIN PR_TABLE2 R ON P.ORDERREF = R.ORDERREF")
    checkForShuffle(partialJoinKeys.logicalPlan, snc, true)

    // More join keys than partitioning keys
    val moreJoinKeys = snc.sql("select P.ORDERREF, P.DESCRIPTION from PR_TABLE1 P JOIN PR_TABLE2 R ON P.ORDERREF = R.ORDERREF AND P.ORDERID = R.ORDERID AND P.DESCRIPTION = R.DESCRIPTION")
    checkForShuffle(moreJoinKeys.logicalPlan, snc, true)


    val leftSemijoinDF = snc.sql("select P.ORDERREF, P.DESCRIPTION from PR_TABLE1 P LEFT SEMI JOIN PR_TABLE2 R ON P.ORDERID = R.ORDERID AND P.ORDERREF = R.ORDERREF")
    checkForShuffle(leftSemijoinDF.logicalPlan, snc, false) // We don't expect a shuffle here
    assert(leftSemijoinDF.count() === 500)

    val innerJoinDF = snc.sql("select P.ORDERREF, P.DESCRIPTION from PR_TABLE1 P INNER JOIN PR_TABLE2 R ON P.ORDERID = R.ORDERID AND P.ORDERREF = R.ORDERREF")
    checkForShuffle(innerJoinDF.logicalPlan, snc, false) // We don't expect a shuffle here
    assert(innerJoinDF.count() === 500)

    val leftJoinDF = snc.sql("select P.ORDERREF, P.DESCRIPTION from PR_TABLE1 P LEFT JOIN PR_TABLE2 R ON P.ORDERID = R.ORDERID AND P.ORDERREF = R.ORDERREF")
    checkForShuffle(leftJoinDF.logicalPlan, snc, false) // We don't expect a shuffle here
    assert(leftJoinDF.count() == 1000)

    val rightJoinDF = snc.sql("select P.ORDERREF, P.DESCRIPTION from PR_TABLE1 P RIGHT JOIN PR_TABLE2 R ON P.ORDERID = R.ORDERID AND P.ORDERREF = R.ORDERREF")
    checkForShuffle(rightJoinDF.logicalPlan, snc, false) // We don't expect a shuffle here
    assert(rightJoinDF.count() == 1000)

    val leftOuterJoinDF = snc.sql("select P.ORDERREF, P.DESCRIPTION from PR_TABLE1 P LEFT OUTER JOIN PR_TABLE2 R ON P.ORDERID = R.ORDERID AND P.ORDERREF = R.ORDERREF")
    checkForShuffle(leftOuterJoinDF.logicalPlan, snc, false) // We don't expect a shuffle here
    assert(leftOuterJoinDF.count() == 1000)

    val rightOuterJoinDF = snc.sql("select P.ORDERREF, P.DESCRIPTION from PR_TABLE1 P RIGHT OUTER JOIN PR_TABLE2 R ON P.ORDERID = R.ORDERID AND P.ORDERREF = R.ORDERREF")
    checkForShuffle(rightOuterJoinDF.logicalPlan, snc, false) // We don't expect a shuffle here
    assert(rightOuterJoinDF.count() == 1000)

    val fullJoinDF = snc.sql("select P.ORDERREF, P.DESCRIPTION from PR_TABLE1 P FULL JOIN PR_TABLE2 R ON P.ORDERID = R.ORDERID AND P.ORDERREF = R.ORDERREF")
    checkForShuffle(fullJoinDF.logicalPlan, snc, false) // We don't expect a shuffle here
    assert(fullJoinDF.count() == 1500)

    val fullOuterJoinDF = snc.sql("select P.ORDERREF, P.DESCRIPTION from PR_TABLE1 P FULL OUTER JOIN PR_TABLE2 R ON P.ORDERID = R.ORDERID AND P.ORDERREF = R.ORDERREF")
    checkForShuffle(fullOuterJoinDF.logicalPlan, snc, false) // We don't expect a shuffle here
    assert(fullOuterJoinDF.count() == 1500)


  }

  // TODO should be enabled after column table schema works is finished
/*  test("PR column table join with PR row Table") {

    val snc = org.apache.spark.sql.SnappyContext(sc)
    val dimension1 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, (i%10 + 1))))
    val refDf = snc.createDataFrame(dimension1)
    snc.sql("DROP TABLE IF EXISTS PR_TABLE1")

    snc.sql("CREATE TABLE PR_TABLE1(OrderId INT,description String, OrderRef INT)" +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId, OrderRef')")


    refDf.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("PR_TABLE1")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE2")

    snc.sql("CREATE TABLE PR_TABLE2(OrderId INT NOT NULL,description String, OrderRef INT)" +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId,OrderRef')")

    val dimension2 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, (i%5 + 1))))

    val dimensionDf = snc.createDataFrame(dimension2)
    dimensionDf.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("PR_TABLE2")


    val countDf = snc.sql("select P.OrderRef, P.description from PR_TABLE1 P JOIN PR_TABLE2 R ON P.OrderId = R.ORDERID AND P.OrderRef = R.ORDERREF")
    val t1 = System.currentTimeMillis()
    assert(countDf.count() === 500) // Make sure aggregation is working with local join
    val t2 = System.currentTimeMillis()
    println("Time taken = "+ (t2-t1))
    val df2 = countDf.head(2)
    df2.foreach(println)

    val semijoinDF = snc.sql("select P.OrderRef, P.description from PR_TABLE1 P LEFT SEMI JOIN PR_TABLE2 R ON P.OrderId = R.ORDERID AND P.OrderRef = R.ORDERREF")
    println(semijoinDF.count())
    assert(countDf.count() === 500)

  }*/
}
