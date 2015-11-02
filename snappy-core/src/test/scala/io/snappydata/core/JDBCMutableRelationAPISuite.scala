package io.snappydata.core

import java.sql.{SQLException, ResultSet, DriverManager}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.streaming.{Seconds, Duration, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.sql.snappy._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.snappy._


import scala.collection.mutable.SynchronizedQueue

/**
 * Created by rishim on 27/8/15.
 */
class JDBCMutableRelationAPISuite extends FunSuite with Logging with BeforeAndAfter{

  private val testSparkContext = SnappySQLContext.sparkContext

  before{
    DriverManager.getConnection("jdbc:derby:./JdbcRDDSuiteDb;create=true")
  }
  after {
    try{
      DriverManager.getConnection("jdbc:derby:./JdbcRDDSuiteDb;shutdown=true")
    }catch{
      // Throw if not normal single database shutdown
      // https://db.apache.org/derby/docs/10.2/ref/rrefexcept71493.html
      case sqlEx : SQLException => {
        if (sqlEx.getSQLState.compareTo("08006") != 0) {
          throw sqlEx
        }
      }
    }

  }

  test("Create table in an external DataStore in Non-Embedded mode") {
    val snc = org.apache.spark.sql.SnappyContext(testSparkContext)

    val props = Map(
      "url" -> "jdbc:derby:./JdbcRDDSuiteDb",
      "driver" -> "org.apache.derby.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )
    snc.sql("DROP TABLE IF EXISTS TEST_JDBC_TABLE_1")

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("jdbc").mode(SaveMode.Overwrite).options(props).saveAsTable("TEST_JDBC_TABLE_1")
    val count = dataDF.count()
    assert(count === data.length)
  }

}
