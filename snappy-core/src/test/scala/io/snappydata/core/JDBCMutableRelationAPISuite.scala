package io.snappydata.core

import java.sql.{DriverManager, SQLException}

import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfterAll

//scalastyle:ignore

import org.apache.spark.sql.SaveMode

/**
 * Tests for non-GFXD JDBC tables.
 *
 * Created by rishim on 27/8/15.
 */
class JDBCMutableRelationAPISuite
    extends SnappyFunSuite
    with BeforeAndAfterAll {

  val path = "JDBCMutableRelationAPISuite"

  override def beforeAll(): Unit = {
    super.beforeAll()
    DriverManager.getConnection(s"jdbc:derby:$path;create=true")
    dirList += path
  }

  override def afterAll(): Unit = {
    try {
      DriverManager.getConnection(s"jdbc:derby:$path;shutdown=true")
    } catch {
      // Throw if not normal single database shutdown
      // https://db.apache.org/derby/docs/10.2/ref/rrefexcept71493.html
      case sqlEx: SQLException =>
        if (sqlEx.getSQLState != "08006" && sqlEx.getSQLState != "XJ015") {
          throw sqlEx
        }
    } finally {
      super.afterAll()
    }
  }

  test("Create table in an external DataStore in Non-Embedded mode") {
    val props = Map(
      "url" -> s"jdbc:derby:$path",
      "driver" -> "org.apache.derby.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )
    snc.sql("DROP TABLE IF EXISTS TEST_JDBC_TABLE_1")

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc().parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("jdbc").mode(SaveMode.Overwrite)
        .options(props).saveAsTable("TEST_JDBC_TABLE_1")
    val count = dataDF.count()
    assert(count === data.length)
  }
}
