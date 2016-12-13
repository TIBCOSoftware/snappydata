package io.snappydata.examples

import java.io.{File, FileInputStream, ByteArrayInputStream}

import scala.io.Source
import scala.sys.process.Process
import scala.sys.process._
import scala.language.postfixOps

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

case class Setup(setupType: String, commands: Array[String])

case class Conf(run: String, setup: Setup, tearDown: String)

case class Examples(examples: Array[Conf])

object ExampleTester {

  implicit class X(in: Seq[String]) {
    // Don't do the BAOS construction in real code.  Just for illustration.
    def pipe(cmd: String) =
      cmd #< new ByteArrayInputStream(in.mkString("\n").getBytes) lineStream
  }

  var snappyHome = ""

  var examples: Array[Conf] = _

  def main(args: Array[String]) {
    snappyHome = args(0)
    startupCluster()
    configureTests
    examples map (c => runSetup(c.setup))
  }

  def configureTests(): Unit = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    val input =
      new FileInputStream("/rishim1/snappy/snappy-commons/examples/src/test/scala/io/snappydata/examples/ExamplesConf.json")
    val confs = objectMapper.readValue(input, classOf[Examples])

    confs.examples.foreach(println)
    examples = confs.examples
  }

  def startupCluster(): Unit ={
    val pb = Process(s"$snappyHome/sbin/snappy-start-all.sh", new File(s"$snappyHome"))
    val exitCode = pb !

  }

  /**
   * Verify that the number of classes in example directory has corresponding entry in ExamplesConf.json
   */
  def verify: Unit = {

  }

  def runSetup(setup :Setup): Unit = {
    val commands = setup.commands
 /*   val commands = Seq("connect client 'localhost:1527';", "DROP TABLE IF EXISTS STAGING_AIRLINE;",
      "CREATE EXTERNAL TABLE STAGING_AIRLINE USING parquet OPTIONS(path \'../../quickstart/data/airlineParquetData\');")*/
    val shell = s"$snappyHome/bin/snappy-shell"
    commands.toSeq pipe shell foreach println
  }

}
