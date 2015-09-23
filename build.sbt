import Dependencies._

lazy val commonSettings = Seq(
  organization := "io.snappydata",
  organizationName := "Snappy Data, Inc.",
  organizationHomepage := Some(url("http://snappydata.io")),
  version := "0.1.0-SNAPSHOT",
  scalaVersion := scalaVer
)

lazy val core = (project in file("snappy-core")).
  settings(commonSettings: _*).
  settings(
    name := "snappy-core",
    description := "Snappy Code that is extension to Spark code. This code depends only on Spark binaries.",
    libraryDependencies  ++=  toolsDeps,
    assemblyJarName in assembly := "snappy-core-assembly-" + version.value + "-" + scalaVersion.value + ".jar"
    // mainClass in assembly := Some("io.snappydata.foo.Main")
 )

lazy val tools = (project in file("snappy-tools")).dependsOn(core).
  settings(commonSettings: _*).
  settings(
    name := "snappy-tools",
    description := "Snappy Code that bridges Spark, GemXD and JobServer. This code depends on Spark, GemXD and JobServer binaries.",
    libraryDependencies  ++= toolsDeps,
    assemblyJarName in assembly := "snappy-tools-assembly-" + version.value + "-" + scalaVersion.value + ".jar"
    // mainClass in assembly := Some("io.snappydata.foo.Main")
 )

lazy val root = (project in file(".")).
  aggregate(core, tools).
  settings(commonSettings: _*)

resolvers in ThisBuild += Resolver.mavenLocal 
resolvers in ThisBuild += "Ooyala Bintray" at "http://dl.bintray.com/ooyala/maven"
resolvers in ThisBuild += "Snappy Local Repo" at "file://" + baseDirectory.value + "/local-repo"

fork in run := true

// This bit of magic from stackoverflow adds the spark jars into the
// runtime classpath for "sbt run", even though the spark dependencies are
// marked as "provided" so that we can build an appropriate application.jar
// for spark-submit.  See the README.md file for more details
// TODO:Soubhik is this required? 
//run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

test in assembly := {} // disable tests in assembly
