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
    snc.setConf("dataFilesLocation", dataFilesLocation)
    CTQueries.snc = snc
    CTQueries.dataFilesLocation = dataFilesLocation
    val tableType = args(1)
    val redundancy = args(2)
    val persistenceMode = args(3)
    val pw = new PrintWriter(new FileOutputStream(new File("CreateAndLoadCTTablesSparkApp.out"),true));
    pw.println(s"dataFilesLocation : ${dataFilesLocation}")
    CTTestUtil.dropTables(snc)
    pw.println(s"Create and load for ${tableType} tables has started")
    tableType match {
      //replicated row tables
      case "Replicated" => CTTestUtil.createReplicatedRowTables(snc)
      case "PersistentReplicated" => CTTestUtil.createPersistReplicatedRowTables(snc,persistenceMode)
      //partitioned row tables
      case "PartitionedRow" => CTTestUtil.createPartitionedRowTables(snc,redundancy)
      case "PersistentPartitionRow" => CTTestUtil.createPersistPartitionedRowTables(snc,redundancy,persistenceMode)
      case "ColocatedRow" => CTTestUtil.createColocatedRowTables(snc,redundancy)
      case "EvictionRow"=> CTTestUtil.createRowTablesWithEviction(snc,redundancy)
      case "PersistentColocatedRow" => CTTestUtil.createPersistColocatedTables(snc,redundancy,persistenceMode)
      case "ColocatedWithEvictionRow" => CTTestUtil.createColocatedRowTablesWithEviction(snc,redundancy,persistenceMode)
      //column tables
      case "Column" => CTTestUtil.createColumnTables(snc,redundancy)
      case "PeristentColumn" => CTTestUtil.createPersistColumnTables(snc,persistenceMode)
      case "ColocatedColumn" => CTTestUtil.createColocatedColumnTables(snc,redundancy)
      case "EvictionColumn" => CTTestUtil.createColumnTablesWithEviction(snc,redundancy)
      case "PersistentColocatedColumn" => CTTestUtil.createPersistColocatedColumnTables(snc,redundancy,persistenceMode)
      case "ColocatedWithEvictionColumn" => CTTestUtil.createColocatedColumnTablesWithEviction(snc,redundancy)
      case _ => pw.println(s"Did not find any match for ${tableType} to create tables") 
    }
    CTTestUtil.loadTables(snc)
    pw.println(s"Create and load for ${tableType} tables has completed successfully")
    pw.close()
  }
}
