package io.snappydata.examples

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkConf, SparkContext}


object SampleApp {
  def main (args: Array[String]) {

    val conf = new SparkConf().setAppName("SampleApp").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val snContext = SnappyContext(sc)
    val externalUrl = "jdbc:snappydata:;"
    val ddlStr = "YearI INT NOT NULL," +
      "MonthI INT NOT NULL," +
      "DayOfMonth INT NOT NULL," +
      "DepTime INT," +
      "ArrTime INT," +
      "UniqueCarrier CHAR(6) NOT NULL"

    snContext.sql(s"create table if not exists airline ($ddlStr) " +
      s" using jdbc options (URL '$externalUrl'," +
      "  Driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver')").collect()

    snContext.sql("insert into airline values(2015, 2, 15, 1002, 1803, 'AA')")
    snContext.sql("insert into airline values(2014, 4, 15, 1324, 1500, 'UT')")

    println("Result of 'SELECT *': ")
    val result = snContext.sql("select * from airline")
    //val expected = Set[Row](Row(2015, 2, 15, 1002, 1803, "AA    "),
    //    Row(2014, 4, 15, 1324, 1500, "UT    "))
    val returnedRows = result.collect().foreach(println)

    val count = snContext.sql("SELECT COUNT(*) FROM airline").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    // This code needs to be removed when we use gemxd for hivemetastore.
    snContext.sql("drop table if exists airline")
  }
}