package io.snappydata.app

import org.scalatest.FunSuite

import org.apache.spark.sql.{Row, SaveMode}

/**
 * Created by vivekb on 27/10/15.
 */
class CreateIndexTest extends FunSuite {

  test("Test create Index on Column Table using Snappy API") {
    val tableName : String = "tcol1"

    val sc = TestSQLContext.sparkContext
    val snContext = org.apache.spark.sql.SnappyContext(sc)

    val props = Map(
      "url" -> "jdbc:snappydata:;mcast-port=33619;user=app;password=app;persist-dd=false",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "user" -> "app",
      "password" -> "app"
    )

    snContext.sql("drop table if exists " + tableName)

    val data = Seq(Seq(111,"aaaaa"), Seq(222,""))
    val rdd = sc.parallelize(data, data.length).map(s => new Data1(s(0).asInstanceOf[Int], s(1).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)
    snContext.createExternalTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    val result = snContext.sql("select col1 from " +
        tableName +
        " where col2 like '%a%'")
    doPrint("")
    doPrint("=============== RESULTS START ===============")
    result.collect.foreach(verifyRows)
    doPrint("=============== RESULTS END ===============")

    try {
      snContext.sql("create index test1 on " + tableName + " (COL1)")
      fail("Should not create index on column table")
    } catch {
      case ae: org.apache.spark.sql.AnalysisException => // ignore
      case e: Exception => throw e
    }
  }

  test("Test create Index on Row Table using Snappy API") {
    val sc = TestSQLContext.sparkContext
    val snContext = org.apache.spark.sql.SnappyContext(sc)
    val tableName : String = "trow1"
    val props = Map(
      "url" -> "jdbc:snappydata:;mcast-port=33619;user=app;password=app;persist-dd=false",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "user" -> "app",
      "password" -> "app"
    )

    snContext.sql("drop table if exists " + tableName)

    val data = Seq(Seq(111,"aaaaa"), Seq(222,""))
    val rdd = sc.parallelize(data, data.length).map(s => new Data1(s(0).asInstanceOf[Int], s(1).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)
    snContext.createExternalTable(tableName, "jdbc", dataDF.schema, props)
    dataDF.write.format("jdbc").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    // TODO fails if column name not in caps
    val result = snContext.sql("select COL1 from " +
        tableName +
        " where COL2 like '%a%'")
    doPrint("")
    doPrint("=============== RESULTS START ===============")
    //result.collect.foreach(println)
    result.collect.foreach(verifyRows)
    doPrint("=============== RESULTS END ===============")

    //snContext.sql("create index on " + tableName)
    snContext.sql("create index test1 on " + tableName + " (COL1)")
  }

  def verifyRows(r: Row) : Unit = {
    doPrint(r.toString())
    assert(r.toString() == "[111]", "got=" + r.toString() + " but expected 111")
  }

  def doPrint(s: String): Unit = {
    println(s)
  }
}
