package org.apache.zeppelin.interpreter;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.spark.repl.SparkILoop;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sachin on 18/6/16.
 *
 */
public class ZeppelinIntpUtil {

  public static Logger logger = LoggerFactory.getLogger(ZeppelinIntpUtil.class);
  private static File tempDir;
  /**
   * This method should be called before spark context is created as zeppelin interpreter will set some properties
   * related to classloader for repl which needs to be specified while creating sparkcontext in lead
   *
   * @return
   */
  public static Properties initializeZeppelinReplAndGetConfig() {
    Properties props=new Properties();

    /**
     * Spark 2.0 REPL is based on scala REPL so http class server is replaced
     * with RPC based file server
     */
    String javaTempDir = System.getProperty("java.io.tmpdir");
    tempDir = Utils.createTempDir(javaTempDir, "repl");
    props.setProperty("spark.repl.class.outputDir", tempDir.getAbsolutePath());
    return props;
  }


  static Object invokeMethod(Object o, String name) {
    return invokeMethod(o, name, new Class[]{}, new Object[]{});
  }

  static Object invokeMethod(Object o, String name, Class[] argTypes, Object[] params) {
    try {
      return o.getClass().getMethod(name, argTypes).invoke(o, params);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      logger.error(e.getMessage(), e);
    }
    return null;
  }


  static Class findClass(String name) {
    return findClass(name, false);
  }

  static Class findClass(String name, boolean silence) {
    try {
      return ZeppelinIntpUtil.class.forName(name);
    } catch (ClassNotFoundException e) {
      if (!silence) {
        logger.error(e.getMessage(), e);
      }
      return null;
    }
  }

  static Object instantiateClass(String name, Class[] argTypes, Object[] params) {
    try {
      Constructor<?> constructor = ZeppelinIntpUtil.class.getClassLoader()
          .loadClass(name).getConstructor(argTypes);
      return constructor.newInstance(params);
    } catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException |
        InstantiationException | InvocationTargetException e) {
      logger.error(e.getMessage(), e);
    }
    return null;
  }

  public static File getTempDir(){

    if (null == tempDir) {
      initializeZeppelinReplAndGetConfig();
    }
    return tempDir;
  }

}