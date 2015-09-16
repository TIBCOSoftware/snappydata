name := "snappy-tools"

description := "Snappy Data Tools Code stitching together all the modules viz. snappy-spark, snapp-core, gemfirexd, jobserver"

organization := "io.snappydata"

organizationName := "Snappy Data, Inc."

organizationHomepage := Some(url("http://snappydata.io"))

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.6"

// If we want to run the spark app by issuing the sbt "run" command, we
// don't want Spark to actually run inside the SBT process / jvm (there are
// bad interactions and I usually get a hot thread or two and have to kill
// things off).  the "fork in run := true" tells SBT to run the program in
// a different proces.
//
// If you aren't running spark, and this seems to be too much of a
// performance hit for you, then you can comment it out.

fork in run := true

// A plausible list of dependencies (For conditional dependencies use BuildSnappy.scala).
libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.2.1",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

// This bit of magic from stackoverflow adds the spark jars into the
// runtime classpath for "sbt run", even though the spark dependencies are
// marked as "provided" so that we can build an appropriate application.jar
// for spark-submit.  See the README.md file for more details
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

// The assembly plugin adds the "assembly" target that builds a jar
// suitable for passing to spark-submit.sh.  The following config
// gives some default names

assemblyJarName in assembly := "snappy-tools-assembly.jar"

// mainClass in assembly := Some("io.snappydata.foo.Main")

test in assembly := {} // disable tests in assembly
