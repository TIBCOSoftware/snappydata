name := "snappy-core"

description := "Core Snappy Data Code"

organization := "io.snappydata"

organizationName := "Snappy Data, Inc."

organizationHomepage := Some(url("http://snappydata.io"))

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.10.4"

val sparkVersion = "1.5.0-SNAPSHOT.1"

// If we want to run the spark app by issuing the sbt "run" command, we
// don't want Spark to actually run inside the SBT process / jvm (there are
// bad interactions and I usually get a hot thread or two and have to kill
// things off).  the "fork in run := true" tells SBT to run the program in
// a different proces.
//
// If you aren't running spark, and this seems to be too much of a
// performance hit for you, then you can comment it out.

fork in run := true

resolvers += "Ooyala Bintray" at "http://dl.bintray.com/ooyala/maven"
externalResolvers += "Local Snappy Maven Repository" at "file://" + baseDirectory.value + "/local-repo"

// A plausible list of dependencies (For conditional dependencies use BuildSnappy.scala).
libraryDependencies ++= Seq(
  "org.apache.spark"         %% "snappy-spark-sql"           % sparkVersion % "provided",
  "org.apache.spark"         %% "snappy-spark-catalyst"      % sparkVersion % "provided",
  "org.apache.spark"         %% "snappy-spark-hive"          % sparkVersion % "provided",
  "org.apache.spark"         %% "snappy-spark-streaming"     % sparkVersion % "provided",
  "org.apache.spark"         %% "snappy-spark-mllib"         % sparkVersion % "provided",

  "com.pivotal"              %% "gemfirexd-client"  % "2.0-Beta"      % "provided",
  "com.pivotal"              %% "gemfirexd"  % "2.0-Beta"      % "provided",
  "com.pivotal"              %% "gemfirexd-tools"  % "2.0-Beta"      % "provided",

  "ooyala.cnd"                % "job-server"      % "0.3.1"      % "provided",
  "org.apache.tomcat"         % "tomcat-jdbc"     % "8.0.24"     % "provided",
  "com.zaxxer"                % "HikariCP-java6"  % "2.3.9"      % "provided",

  "com.typesafe" % "config" % "1.2.1",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.scala-lang"            % "scala-actors"               % scalaVersion.value % "provided",
  "org.scala-lang"            % "scala-reflect"              % scalaVersion.value % "provided"

)

// This bit of magic from stackoverflow adds the spark jars into the
// runtime classpath for "sbt run", even though the spark dependencies are
// marked as "provided" so that we can build an appropriate application.jar
// for spark-submit.  See the README.md file for more details
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

// The assembly plugin adds the "assembly" target that builds a jar
// suitable for passing to spark-submit.sh.  The following config
// gives some default names

assemblyJarName in assembly := "snappy-core-assembly.jar"

// mainClass in assembly := Some("io.snappydata.foo.Main")

test in assembly := {} // disable tests in assembly
