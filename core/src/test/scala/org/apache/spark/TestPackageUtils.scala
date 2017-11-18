package org.apache.spark

import java.io.File

object TestPackageUtils {

  val userDir = System.getProperty("user.dir")

  val pathSeparator = File.pathSeparator

  def destDir: File = {
    val jarDir = new File(s"$userDir/jars")
    if (!jarDir.exists()) {
      jarDir.mkdir()
    }
    jarDir
  }

  def createJarFile(files: Seq[File], filePrefix: Option[String] = None): String = {
    val jarFile = new File(destDir, "testJar-%s.jar".format(System.currentTimeMillis()))
    TestUtils.createJar(files, jarFile, filePrefix)
    jarFile.getPath
  }
}
