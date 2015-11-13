package org.apache.spark.sql.store

import io.snappydata.core.{TestData2, RefData, TestData, TestSqlContext, FileCleaner}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkContext, Logging}

/**
 * Created by rishim on 6/11/15.
 */
class SnappyJoinSuite extends FunSuite with Logging  with BeforeAndAfterAll{

  var sc : SparkContext= null

  override def afterAll(): Unit = {
    sc.stop()
    //
    //FileCleaner.cleanStoreFiles()
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
    val t1 = System.currentTimeMillis()
    assert(countDf.count() === 1000) // Make sure aggregation is working with local join
    val t2 = System.currentTimeMillis()
    println("Time taken = "+ (t2-t1))

    val projectDF = snc.sql("select R.ORDERID, P.DESCRIPTION , R.DESCRIPTION from RR_TABLE1 P JOIN RR_TABLE2 R ON P.ORDERREF = R.ORDERREF")
    assert(projectDF.columns.length === 3)

    val sumDF = snc.sql("select SUM(R.ORDERID)from RR_TABLE1 P JOIN RR_TABLE2 R ON P.ORDERREF = R.ORDERREF")
    assert(sumDF.collect()(0).getLong(0) === ((1000 * 1001)/2))

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


    val countDf = snc.sql("select P.ORDERREF, P.DESCRIPTION from PR_TABLE1 P JOIN PR_TABLE2 R ON P.ORDERID = R.ORDERID AND P.ORDERREF = R.ORDERREF")
    val t1 = System.currentTimeMillis()
    assert(countDf.count() === 500) // Make sure aggregation is working with local join
    val t2 = System.currentTimeMillis()
    println("Time taken = "+ (t2-t1))
    val df2 = countDf.head(2)
    df2.foreach(println)

    val semijoinDF = snc.sql("select P.ORDERREF, P.DESCRIPTION from PR_TABLE1 P LEFT SEMI JOIN PR_TABLE2 R ON P.ORDERID = R.ORDERID AND P.ORDERREF = R.ORDERREF")
    println(semijoinDF.count())
    assert(countDf.count() === 500)

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
