package io.snappydata.dunit.cluster

import scala.math._
import scala.util.Random

import org.apache.spark.sql.{Row, SnappyContext}

class ClusterMgrDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  import ClusterMgrDUnitTest._

  /**
   * This test starts a lead node and two server nodes. Executes a job.
   * Then stops the lead node and starts lead in another node and then executes
   * the same job.
   */
  def testMultipleDriver(): Unit = {
    // Execute the job
    startSparkJob()
    startGemJob()
    // Stop the lead node
    ClusterManagerTestBase.stopSpark()

    // Start the lead node in another JVM. The executors should
    // connect with this new lead.
    // In this case servers are already running and a lead comes
    // and join
    vm3.invoke(getClass, "startSnappyLead", startArgs)
    vm3.invoke(getClass, "startSparkJob")
    vm3.invoke(getClass, "startGemJob")
    vm3.invoke(getClass, "stopSpark")
  }
}

object ClusterMgrDUnitTest {

  private def sc = SnappyContext.globalSparkContext

  def startSparkJob(): Unit = {
    val slices = 5
    val n = math.min(1000000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = sc.parallelize(1 until n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x * x + y * y < 1) 1 else 0
      }.reduce(_ + _)
    val pi = 4.0 * count / n
    assert(3.04 <= pi)
    assert(3.25 > pi)
  }

  def startGemJob(): Unit = {

    val snContext = SnappyContext(sc)
    val externalUrl = "jdbc:snappydata:;"
    val ddlStr = "YearI INT NOT NULL," +
        "MonthI INT NOT NULL," +
        "DayOfMonth INT NOT NULL," +
        "DepTime INT," +
        "ArrTime INT," +
        "UniqueCarrier CHAR(6) NOT NULL"

    if (new Random().nextBoolean()) {
      snContext.sql("drop table if exists airline")
      snContext.sql(s"create table airline ($ddlStr) " +
          s" using jdbc options (URL '$externalUrl'," +
          "  Driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver')").collect()
    } else {
      snContext.sql(s"create table if not exists airline ($ddlStr) " +
          s" using jdbc options (URL '$externalUrl'," +
          "  Driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver')").collect()
    }

    snContext.sql("insert into airline values(2015, 2, 15, 1002, 1803, 'AA')")
    snContext.sql("insert into airline values(2014, 4, 15, 1324, 1500, 'UT')")

    val result = snContext.sql("select * from airline")
    val expected = Set[Row](Row(2015, 2, 15, 1002, 1803, "AA    "),
        Row(2014, 4, 15, 1324, 1500, "UT    "))
    val returnedRows = result.collect()
    println(s"Returned rows: ${returnedRows.mkString(",")} ")
    println(s"Expected rows: ${expected.mkString(",")}")
    assert(returnedRows.toSet == expected)

    snContext.sql("drop table if exists airline")
  }
}
