package org.apache.spark.sql.store

import java.io._
import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.pivotal.gemfirexd.FabricService
import io.snappydata.{Constant, ServiceManager, Server, SnappyFunSuite}
import io.snappydata.core.Data
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.scalatest.{FunSuite, BeforeAndAfterAll, BeforeAndAfter}

import org.apache.spark.sql.{AnalysisException, SaveMode}
import util.TestException
import scala.language.implicitConversions

import scala.sys.process._


class Snap_213
  extends FunSuite
  with BeforeAndAfter
  with BeforeAndAfterAll {


  test("Tes gemxd simple Prepare Statement with bytes") {
    //shouldn't be able to create without schema


    //start spark cluster

    // startSparkCluster

    //start locator
    /*    val locator = ServiceManager.getLocatorInstance
        locator.start("localhost" , 10334 , null)

        locator.startNetworkServer("localhost" , 1527 , null)*/


    //start server
    val props: Properties = new Properties()
    props.put("host-data", "true")
    val server: Server = ServiceManager.getServerInstance
    server.start(props)
    server.startNetworkServer("localhost", 1560, null)

    println("server  started ")
    //create client connection and load data
    val obj: ObjectInputStream = new ObjectInputStream(new FileInputStream(new File("/home/namrata/serializedObject.ser")))
    val myObject = obj.readObject().asInstanceOf[Array[Array[Byte]]]

    DriverRegistry.register(Constant.JDBC_CLIENT_DRIVER);
    val conn: Connection = DriverManager.getConnection("jdbc:snappydata://localhost:1560")


    val tableName = "TEST_TABLE"
    //conn.createStatement().execute("drop table if exists airline " )

    conn.createStatement().execute("create table " + tableName + " (uuid varchar(36) not null, bucketId integer, stats blob, " +
      "Col_Year blob,Col_Month blob,Col_DayOfMonth blob,Col_DayOfWeek blob,Col_DepTime blob,Col_CRSDepTime blob," +
      " Col_ArrTime blob,Col_CRSArrTime blob,Col_UniqueCarrier blob,Col_FlightNum blob,Col_TailNum blob,Col_ActualElapsedTime blob, " +
      " Col_CRSElapsedTime blob,Col_AirTime blob,Col_ArrDelay blob,Col_DepDelay blob,Col_Origin blob,Col_Dest blob,Col_Distance blob,Col_TaxiIn blob," +
      "Col_TaxiOut blob,Col_Cancelled blob,Col_CancellationCode blob,Col_Diverted blob,Col_CarrierDelay blob,Col_WeatherDelay blob," +
      "Col_NASDelay blob,Col_SecurityDelay blob,Col_LateAircraftDelay blob,Col_ArrDelaySlot blob , " +
      //"constraint airline_bucketCheck check (bucketId != -1)," +
      "primary key (uuid, bucketId)) partition by column(bucketId)")

    val stmt = conn.prepareStatement("insert into " + tableName + " values ( " + "? " + " , ? " * 32 + ") ")


    stmt.setString(1, " first row")
    stmt.setInt(2, 1)
    stmt.setBytes(3, null)
    var index = 4
    myObject.foreach(buffer => {
      //works for first 10. fails for 15
      if (index <= 13)
        stmt.setBytes(index, buffer)
      else {
        stmt.setBytes(index, null)
      }
      index = index + 1
    }
    )


    stmt.execute()

    conn.close()
    server.stop(null)

  }

  def getEnvironmentVariable(env: String): String = {
    val value = scala.util.Properties.envOrElse(env, null)
    if (env == null)
      throw new TestException(s" Environment variable $env is not defined")
    value
  }

  def startSparkCluster = {
    (getEnvironmentVariable("SNAPPY_HOME") + "/sbin/start-all.sh") !!
  }

  def stopSparkCluster = {
    (getEnvironmentVariable("SNAPPY_HOME") + "/sbin/stop-all.sh") !!
  }

}
