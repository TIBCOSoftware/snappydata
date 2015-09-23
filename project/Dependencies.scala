import sbt._

object Dependencies {


  val sparkVersion = "1.5.0-SNAPSHOT.1"  
  val scalaVer = "2.10.4"

  lazy val sparkSql = "org.apache.spark" %% "snappy-spark-sql" % sparkVersion % "provided"
  lazy val sparkCatalyst = "org.apache.spark" %% "snappy-spark-catalyst" % sparkVersion % "provided"
  lazy val sparkHive = "org.apache.spark" %% "snappy-spark-hive" % sparkVersion % "provided"
  lazy val sparkStreaming = "org.apache.spark" %% "snappy-spark-streaming" % sparkVersion % "provided"
  lazy val sparkMllib = "org.apache.spark" %% "snappy-spark-mllib" % sparkVersion % "provided"

  lazy val gemxdClient = "com.pivotal" %% "gemfirexd-client" % "2.0-Beta" % "provided"
  lazy val gemxd = "com.pivotal" %% "gemfirexd" % "2.0-Beta" % "provided"
  lazy val gemxdTools = "com.pivotal" %% "gemfirexd-tools" % "2.0-Beta" % "provided"

  lazy val jobServer = "ooyala.cnd" % "job-server" % "0.3.1" % "provided"
  lazy val tomcat = "org.apache.tomcat" % "tomcat-jdbc" % "8.0.24" % "provided"
  lazy val hikariCP = "com.zaxxer" % "HikariCP-java6" % "2.3.9" % "provided"

  lazy val typesafeConfig = "com.typesafe" % "config" % "1.2.1"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "2.2.1" % "test"
  lazy val scalaActors = "org.scala-lang" % "scala-actors" % scalaVer % "provided"
  lazy val scalaReflect = "org.scala-lang" % "scala-reflect" % scalaVer % "provided"

  lazy val commonDeps = Seq(scalaTest, scalaActors, scalaReflect, typesafeConfig, 
                     sparkSql, sparkCatalyst, sparkHive, sparkStreaming, sparkMllib)

  lazy val toolsDeps = Seq(gemxdClient, gemxd, gemxdTools, jobServer, tomcat, hikariCP) ++ commonDeps

  lazy val coreDeps = commonDeps 

}
