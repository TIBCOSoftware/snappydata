import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._


object SnappyBuild extends Build {

  val sparkVersion = "1.3.0"

  val localSparkBuild = Option(sys.props.getOrElse("LOCAL_SPARK_BUILD", sys.env.getOrElse("LOCAL_SPARK_BUILD", null)))

  val depSettings =
    if (localSparkBuild.isEmpty) {
      Seq(libraryDependencies ++= Seq(
        // Uncomment this if/when you want to use snappyutils, but you'll have
        // to first build and publishLocal snappyutils project
        //  "io.snappydata"            %% "snappyutils"     % "0.0.1-SNAPSHOT",
        "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
        "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
      ))
    }
    else {
      def targets(tgt: File) = ((tgt / "target") * "*") filter { f => f.name.startsWith("scala-") && f.isDirectory }
      val level1dirs = file(localSparkBuild.get) * "*" filter (_.isDirectory)

      val sparkBuildJars = ((level1dirs.flatMap(targets(_)) +++ (level1dirs ** "*").filter(_.isDirectory).flatMap(targets(_))) ** "*.jar")
        .classpath

      Seq(
        unmanagedJars in Compile ++= sparkBuildJars,
        assemblyExcludedJars in assembly ++= sparkBuildJars
      )
    }

  val xyz = Project("snappy-core", file("."), settings = depSettings)
}

