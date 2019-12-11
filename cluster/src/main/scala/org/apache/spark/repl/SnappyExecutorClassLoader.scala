package org.apache.spark.repl

import java.io._

import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector
import com.pivotal.gemfirexd.internal.engine.distributed.message.GetLeadNodeInfoMsg
import com.pivotal.gemfirexd.internal.engine.distributed.message.GetLeadNodeInfoMsg.DataReqType
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkEnv}

class SnappyExecutorClassLoader(conf: SparkConf,
      env: SparkEnv,
      classUri: String,
      parent: ClassLoader,
      userClassPathFirst: Boolean) extends ExecutorClassLoader(
        conf, env, classUri, parent, userClassPathFirst) {

  lazy val tmpDestDir = getOrCreateTempLocationForThisLoader

  private def getOrCreateTempLocationForThisLoader: String = {
    val lastIndexSlash = classUri.lastIndexOf('/')
    val replOutputDir = classUri.substring(lastIndexSlash+1)
    val tmpDir = env.conf.get("spark.local.dir", "/tmp")
    val dirPath = s"$tmpDir/$replOutputDir"
    FileUtils.deleteDirectory(new File(dirPath))
    if (!(new File(dirPath).mkdir())) {
      throw new RuntimeException(s"Could not create tmp directory for repl $classUri")
    }
    dirPath
  }

  override def findClassLocally(name: String): Option[Class[_]] = {
    val pathInDirectory = name.replace('.', '/') + ".class"
    var inputStream: InputStream = null
    try {
      val fullPath = s"$classUri/$pathInDirectory"
      inputStream = pullFromLead(name, fullPath, s"$tmpDestDir/$name")
      val bytes = readAndTransformClass(name, inputStream)
      Some(defineClass(name, bytes, 0, bytes.length))
    } catch {
      case e: ClassNotFoundException =>
        // We did not find the class
        logInfo(s"Did not load class $name from REPL class server at $uri", e)
        None
      case e: Exception =>
        // Something bad happened while checking if the class exists
        logError(s"Failed to check existence of class $name on REPL class server at $uri", e)
        None
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close()
        } catch {
          case e: Exception =>
            logError("Exception while closing inputStream", e)
        }
      }
    }
  }

  private def pullFromLead(name: String, sourcePath: String, destPath: String) = {
    val collector = new GfxdListResultCollector
    val fetchClassByteMsg = new GetLeadNodeInfoMsg(
      collector, DataReqType.GET_CLASS_BYTES, 0L, sourcePath)
    fetchClassByteMsg.executeFunction();
    val result = collector.getResult.get(0)
    logInfo(s"KN: resule obtained = $result")
    val fileContent = result.asInstanceOf[Array[Byte]]
    new ByteArrayInputStream(fileContent)
  }
}
