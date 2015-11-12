package io.snappydata.app

import java.io._
import java.net._

import scala.actors.threadpool.AtomicInteger
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.io._
import scala.reflect.io.File

/*
 * Reads tuples from files and streams them to clients
 * If only one file is given in command line then the same file is
 * streamed to all the clients. If more than 1 file is given (comma, separated)
 * then every client is streamed data in sequential order.
 * For example if there are 3 input files then contents will be streamed as follows
 * client 1 - file1
 * client 2 - file2
 * client 3 - file3
 * client 4 - file1
 * ...
 * ...
 */
object StreamingTupleGenerator {

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: StreamingTupleGenerator " +
          "<port-number> <comma-seperated-filenames>")
      System.exit(1)
    }

    val Array(port, files@_*) = args

    val fileArr = files map {
      File(_).toFile
    }

    println(s"scanning files ")
    fileArr.foreach(println)

    val server = new ServerSocket(port.toInt)
    println("created server socket on port: " + port.toInt)

    val curr = new AtomicInteger(0)
    val counter = new AtomicInteger(0)

    while (true) {
      println("Waiting for client on")

      val clientSock = server.accept()

      println("accepted client connection " + clientSock)

      val out = new PrintStream(clientSock.getOutputStream)

      try {
        val f = Future {

          while (true) {
            val idx = curr.getAndAdd(1)
            if (idx == fileArr.length - 1) {
              println("Round done.. press any key for next round.")
              readLine()
              counter.addAndGet(1)
              curr.set(0)
            }

            new OneFileGenerator(out, fileArr(idx)).run()
          }
        }

        Await.result(f, scala.concurrent.duration.Duration.Inf)

        f onComplete {
          case scala.util.Success(v) =>
            println(s"$clientSock connection terminated ")
          case scala.util.Failure(t) =>
            println(s"$clientSock connection aborted $t")
            t.printStackTrace()
          case _ => sys.error("unknown state...")
        }
      } finally {
        println("CLOSING....")
        out.close()
        clientSock.close()
      }
    }
    server.close()
  }

  class OneFileGenerator[T >: File](out: PrintStream, file: T) {
    //extends Thread {
    val src = Source.fromFile(file.toString).getLines()
    println("Sending tuples from file = " + file)

    def run(): Unit = {
      //val begin = System.currentTimeMillis()
      for (l <- src) {
        // println("sending line = " + l)
        out.println(l)
      }
      out.flush()
    }
  }
}
