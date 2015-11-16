package io.snappydata.app

import java.sql.DriverManager
import java.util.Properties

import com.pivotal.gemfirexd.FabricServiceManager
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._
import org.apache.spark.sql.collection.ReusableRow

object ExternalStoreTest extends App {

  def addArrDelaySlot(row: ReusableRow, arrDelayIndex: Int,
      arrDelaySlotIndex: Int): Row = {
    val arrDelay =
      if (!row.isNullAt(arrDelayIndex)) row.getInt(arrDelayIndex) else 0
    row.setInt(arrDelaySlotIndex, math.abs(arrDelay) / 10)
    row
  }

  val p:Properties = new Properties()
  FabricServiceManager.getFabricLocatorInstance.start("localhost",10332,p)
  p.setProperty("locators" , "localhost:10332")
  val server = FabricServiceManager.getFabricServerInstance
    server.start(p)
  server.startNetworkServer("localhost" , 1530, null)

  val conf = new SparkConf().
    setAppName("test Application")
    .setMaster(s"external:snappy:spark://pnq-nthanvi02:1527")
    .set("snappy.locator", s"localhost[1527]")
  //switching to http broadcast as could not run jobs with TorrentBroadcast
  // .set("spark.broadcast.factory" , "org.apache.spark.broadcast.HttpBroadcastFactory")


  val sc = new SparkContext(conf)


  val snc = org.apache.spark.sql.SnappyContext(sc)




  def option(list: List[String]): Boolean = {
    list match {
      case "-hfile" :: value :: tail =>
        hfile = value
        print(" hfile " + hfile)
        option(tail)
      case "-noload" :: tail =>
        loadData = false
        print(" loadData " + loadData)
        option(tail)
      case "-set-master" :: value :: tail =>
        setMaster = value
        print(" setMaster " + setMaster)
        option(tail)
      case "-nomaster" :: tail =>
        setMaster = null
        print(" setMaster " + setMaster)
        option(tail)
      case "-set-jars" :: value :: tail =>
        setJars = value
        print(" setJars " + setJars)
        option(tail)
      case "-executor-extraClassPath" :: value :: tail =>
        executorExtraClassPath = value
        print(" executor-extraClassPath " + executorExtraClassPath)
        option(tail)
      case "-executor-extraJavaOptions" :: value :: tail =>
        executorExtraJavaOptions = value
        print(" executor-extraJavaOptions " + executorExtraJavaOptions)
        option(tail)
      case "-debug" :: tail =>
        debug = true
        print(" debug " + debug)
        option(tail)
      case opt :: tail =>
        println(" Unknown option " + opt)
        sys.exit(1)
      case Nil => true
    }
  } // end of option
}
