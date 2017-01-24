package io.snappydata.hydra.distIndex

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySession, SnappySQLJob}

import scala.util.{Failure, Success, Try}

class distIndexTest3Job extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val pw: PrintWriter = new PrintWriter(new FileOutputStream(new File(jobConfig.getString("logFileName"))), true)
    Try {
      pw.println("****** distIndexTest1Job : Test choice of index according to where predicate started ******")
      snc.sql("drop index if exists indexThree")
      snc.sql("drop index if exists indexTwo")
      snc.sql("drop index if exists indexOne")
      snc.sql("drop table if exists tabOne")
      snc.sql("create table tabOne(id int, name String, address String) USING column OPTIONS(partition_by 'id')")
      snc.sql("insert into tabOne values(111, 'aaa', 'hello')")
      snc.sql("insert into tabOne values(222, 'bbb', 'halo')")
      snc.sql("insert into tabOne values(333, 'aaa', 'hello')")
      snc.sql("insert into tabOne values(444, 'bbb', 'halo')")
      snc.sql("insert into tabOne values(555, 'ccc', 'halo')")
      snc.sql("insert into tabOne values(666, 'ccc', 'halo')")
      snc.sql("create index indexOne on tabOne (id)")
      snc.sql("create index indexTwo on tabOne (name, address)")
      snc.sql("create index indexThree on tabOne (id, address)")
      val q1 = "select * from tabOne where id = 111"
      val q2 = "select * from tabOne where name = 'aaa'"
      val q3 = "select * from tabOne where name = 'bbb' and address = 'halo'"
      val q4 = "select * from tabOne where id = 111 and address = 'halo'"
      var df = snc.sql(q1)
      pw.println(q1 + ":\n" + df.show())
      df = snc.sql(q2)
      pw.println(q2 + ":\n" + df.show())
      df = snc.sql(q3)
      pw.println(q3 + ":\n" + df.show())
      df = snc.sql(q4)
      pw.println(q4 + ":\n" + df.show())
      pw.println("****** distIndexTest1Job : Test choice of index according to where predicate finished ******")
      return String.format("See %s/" + jobConfig.getString("logFileName"), getCurrentDirectory)
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${jobConfig.getString("logFileName")}"
      case Failure(e) =>
        pw.println("Exception occurred while executing the job " + "\nError Message:" + e.getMessage)
        pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
