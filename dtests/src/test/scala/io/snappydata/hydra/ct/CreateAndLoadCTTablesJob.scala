package io.snappydata.hydra.ct

import java.io.{File, FileOutputStream, PrintWriter}

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config

import org.apache.spark.sql.{SnappyContext, SnappyJobValid, SnappyJobValidation, SnappySQLJob}

class CreateAndLoadCTTablesJob extends SnappySQLJob {

  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    val pw = new PrintWriter(new FileOutputStream(new File("CreateAndLoadCTTablesJob.out"), true));
    val tableType = jobConfig.getString("tableType")
    pw.println("In create and load tables Job")
    Try {
      snc.sql("set spark.sql.shuffle.partitions=6")
      val dataFilesLocation = jobConfig.getString("dataFilesLocation")
      pw.println(s"Data files are at : ${dataFilesLocation}")
      CTQueries.snc = snc
      CTQueries.dataFilesLocation = dataFilesLocation
      CTTestUtil.dropTables(snc)
      pw.println(s"Create and load for ${tableType} tables has started...")

      tableType match {
          //row tables
        case "Replicated" => CTTestUtil.createReplicatedRowTables(pw,snc)
        case "PersistentReplicated" => CTTestUtil.createPersistReplicatedRowTables(snc)
          //partitioned tables
        case "PartitionedRow" => CTTestUtil.createPartitionedRowTables(snc)
        case "PersistentPartitionRow" => CTTestUtil.createPersistPartitionedRowTables(snc)
        case "ColocatedRow" => CTTestUtil.createColocatedRowTables(snc)
        case "RowWithEviction"=> CTTestUtil.createRowTablesWithEviction(snc)
        case "PersistentColocatedRow" => CTTestUtil.createPersistColocatedTables(snc)
        case "ColocatedWithEvictionRow" => CTTestUtil.createColocatedRowTablesWithEviction(snc)
          //column tables
        case "Column" => CTTestUtil.createColumnTables(snc)
        case "PeristentColumn" => CTTestUtil.createPersistColumnTables(snc)
        case "ColocatedColumn" => CTTestUtil.createColocatedColumnTables(snc)
        case "ColumnWithEviction" => CTTestUtil.createColumnTablesWithEviction(snc)
        case "PersistentColocatedColumn" => CTTestUtil.createPersistColocatedColumnTables(snc)
        case "ColocatedWithEvictionColumn" => CTTestUtil.createColocatedColumnTablesWithEviction(snc)
        case _ => // the default, catch-all
      }
      CTTestUtil.loadTables(snc);
      pw.println(s"Create and load ${tableType} tables completed.")
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${CTTestUtil.getCurrentDirectory}/CreateAndLoadCTTablesJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()
}
