package org.apache.spark.sql.store

import io.snappydata.SnappyFunSuite
import io.snappydata.core.Data
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{SaveMode, Row, DataFrame}
import org.apache.spark.sql.functions._

class CubeRollupGroupingSetsTest extends SnappyFunSuite with BeforeAndAfterAll {
  private var testData: DataFrame = _

  override def beforeAll() {
    val data = Seq(Seq(1, 2005, 12000), Seq(1, 2006, 18000), Seq(1, 2007, 25000), Seq(2, 2005, 15000), Seq(2, 2006, 6000), Seq(2, 2007, 25000))
    val testRDD = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    testData = snc.createDataFrame(testRDD)
    snc.sql("Create table mytable (col1 INT, col2 INT, col3 INT)")
    testData.write.format("row").mode(SaveMode.Append).saveAsTable("mytable")
  }

  override def afterAll(): Unit = {
    snc.sql("drop table mytable")
  }

  test("snappy cube_rollup query") {
    //group by
    val dfGroupByResult = testData.groupBy("col1", "col2").agg(sum("col2")).orderBy("col1", "col2").collect()
    val snappyGroupByResult = snc.sql("select col1, col2, sum(col2) from mytable " +
        "group by col1, col2 order by col1, col2").collect()

    println("DataFrame group by result")
    dfGroupByResult.foreach(println)
    println("SnappySQL group by result")
    snappyGroupByResult.foreach(println)
    assert(dfGroupByResult.sameElements(snappyGroupByResult))

    //roll up
    val dfRollupResult = testData.rollup("col1", "col2").agg(sum("col3")).orderBy("col1", "col2").collect()
    val snappyRollupResult = snc.sql("select col1, col2, sum(col3) from mytable group by col1, col2 " +
        "with rollup order by col1, col2").collect()

    println("DataFrame rollup result")
    dfRollupResult.foreach(println)
    println("SnappySQL rollup result")
    snappyRollupResult.foreach(println)
    assert(dfRollupResult.sameElements(snappyRollupResult))

    // cube
    val dfCubeResult = testData.cube("col1", "col2").agg(sum("col3")).orderBy("col1", "col2").collect()
    val snappyCubeResult = snc.sql("select col1, col2, sum(col3) from mytable group by col1, col2 " +
        "with cube order by col1, col2").collect()

    println("DataFrame cube result")
    dfCubeResult.foreach(println)
    println("SnappySQL cube result")
    snappyCubeResult.foreach(println)
    assert(dfCubeResult.sameElements(snappyCubeResult))

    // grouping sets query equivalent to above cube query
    val snappyGoupingSetResult = snc.sql("select col1, col2, sum(col3) from mytable group by col1, col2 " +
        "with grouping sets ((col1, col2), (col1), (col2), ()) order by col1, col2").collect()
    println("DataFrame cube result")
    dfCubeResult.foreach(println)
    println("SnappySQL gouping sets result")
    snappyGoupingSetResult.foreach(println)
    assert(dfCubeResult.sameElements(snappyGoupingSetResult))

  }
}
