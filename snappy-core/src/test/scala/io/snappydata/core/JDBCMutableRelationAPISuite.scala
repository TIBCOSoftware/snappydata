package io.snappydata.core

import java.sql.{DriverManager, SQLException}

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.sql.SaveMode

/**
 * Created by rishim on 27/8/15.
 */
class JDBCMutableRelationAPISuite extends FunSuite with Logging with BeforeAndAfter with BeforeAndAfterAll{

  var sc : SparkContext= null

  override def afterAll(): Unit = {
    sc.stop()

    try {
      DriverManager.getConnection("jdbc:derby:target/JDBCMutableRelationAPISuite;shutdown=true")
    } catch {
      // Throw if not normal single database shutdown
      // https://db.apache.org/derby/docs/10.2/ref/rrefexcept71493.html
      case sqlEx: SQLException => {
        if (sqlEx.getSQLState.compareTo("08006") != 0) {
          throw sqlEx
        }
      }
    }
  }

  override def beforeAll(): Unit = {
    sc = new LocalSQLContext().sparkContext
    DriverManager.getConnection("jdbc:derby:target/JDBCMutableRelationAPISuite;create=true")
  }

  test("Create table in an external DataStore in Non-Embedded mode") {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val props = Map(
      "url" -> "jdbc:derby:target/JDBCMutableRelationAPISuite",
      "driver" -> "org.apache.derby.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )
    snc.sql("DROP TABLE IF EXISTS TEST_JDBC_TABLE_1")

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("jdbc").mode(SaveMode.Overwrite).options(props).saveAsTable("TEST_JDBC_TABLE_1")
    val count = dataDF.count()
    assert(count === data.length)
  }

}
