package io.snappydata.hydra.ct

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkContext, SparkConf}

object CreateAndLoadCTTablesApp {
  val conf = new SparkConf().
      setAppName("CTTestUtil Application")
  val sc = new SparkContext(conf)
  val snc = SnappyContext(sc)

  def main(args: Array[String]) {
    val dataFilesLocation = args(0)
    CTQueries.snc = snc
    CTQueries.dataFilesLocation = dataFilesLocation
    val tableType = args(1)
    val pw = new PrintWriter(new FileOutputStream(new File("CreateAndLoadCTTablesSparkApp.out"),true));
    pw.println(s"dataFilesLocation : ${dataFilesLocation}")
    CTTestUtil.dropTables(snc)
    pw.println(s"Create and load for ${tableType} tables has started")
    tableType match {
      case "Replicated" => CTTestUtil.createReplicatedRowTables(pw,snc)
      case "PersistentReplicated" => CTTestUtil.createPersistReplicatedRowTables(snc)
      //partitioned tables
      case "PartitionedRow" => CTTestUtil.createPartitionedRowTables(snc)
      case "PersistentPartitionRow" => CTTestUtil.createPersistPartitionedRowTables(snc)
      case "ColocatedRow" => CTTestUtil.createColocatedRowTables(snc)
      case "EvictionRow"=> CTTestUtil.createRowTablesWithEviction(snc)
      case "PersistentColocatedRow" => CTTestUtil.createPersistColocatedTables(snc)
      case "ColocatedWithEvictionRow" => CTTestUtil.createColocatedRowTablesWithEviction(snc)
      //column tables
      case "Column" => CTTestUtil.createColumnTables(snc)
      case "PeristentColumn" => CTTestUtil.createPersistColumnTables(snc)
      case "ColocatedColumn" => CTTestUtil.createColocatedColumnTables(snc)
      case "EvictionColumn" => CTTestUtil.createColumnTablesWithEviction(snc)
      case "PersistentColocatedColumn" => CTTestUtil.createPersistColocatedColumnTables(snc)
      case "ColocatedWithEvictionColumn" => CTTestUtil.createColocatedColumnTablesWithEviction(snc)
      case _ => // the default, catch-all
    }
    CTTestUtil.loadTables(snc)
    pw.println(s"Create and load for ${tableType} tables has completed successfully")
    pw.close()
  }
}
