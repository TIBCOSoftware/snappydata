package io.snappydata.hydra.putInto

import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.Config
import io.snappydata.hydra.northwind.NWTestJob
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

/**
  * Created by swati on 22/11/18.
  */
object ConcurrentPutInto extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val pw = new PrintWriter(new FileOutputStream(new File("ConcurrentPutIntoJob.out"), true));
    Try {
  val globalId = new AtomicInteger()
  val doPut = () => Future {
    val conn = DriverManager.getConnection("jdbc:snappydata://localhost:1527")
    val stmt = conn.createStatement()
    val myId = globalId.getAndIncrement()
    val blockSize = 100000L
    val stepSize = 50000L
    for (i <- (myId * 1000) until 1000000) {
      stmt.executeUpdate("put into testL select id, " +
          "'biggerDataForInsertsIntoTheTable1_' || id, id * 10.2 " +
          s"from range(${i * stepSize}, ${i * stepSize + blockSize})")
    }
    stmt.close()
    conn.close()
  }
  val doQuery = () => Future {
    val conn = DriverManager.getConnection("jdbc:snappydata://localhost:1527")
    val stmt = conn.createStatement()
    val myId = globalId.getAndIncrement()
    for (i <- 0 until 10000000) {
      stmt.executeQuery("select avg(id), max(data), last(data2) from testL " +
          s"where id <> ${myId + i}")
    }
    stmt.close()
    conn.close()
  }
  val putTasks = Array.fill(8)(doPut())
  val queryTasks = Array.fill(8)(doQuery())

  putTasks.foreach(Await.result(_, Duration.Inf))
  queryTasks.foreach(Await.result(_, Duration.Inf))

      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${NWTestJob.getCurrentDirectory}/ConcurrentPutIntoJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
