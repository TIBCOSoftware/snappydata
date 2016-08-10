package org.apache.zeppelin.interpreter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.spark.HttpServer;
import org.apache.spark.repl.SparkILoop;

/**
 * Created by sachin on 18/6/16.
 *
 */
public class ZeppelinIntpUtil {
  /**
   * This method should be called before spark context is created as zeppelin interpreter will set some properties
   * related to classloader for repl which needs to be specified while creating sparkcontext in lead
   *
   * @return
   */
  public static Properties initializeZeppelinReplAndGetConfig() {
    Properties props=new Properties();

    SnappyDataZeppelinInterpreter snappyZeppelinIntp = new SnappyDataZeppelinInterpreter(new Properties());
    SparkILoop interpreter  = snappyZeppelinIntp.getReplInterpreter();
    String classServerUri = null;
    try { // in case of spark 1.1x, spark 1.2x
      Method classServer = interpreter.intp().getClass().getMethod("classServer");
      HttpServer httpServer = (HttpServer) classServer.invoke(interpreter.intp());
      classServerUri = httpServer.uri();
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException
        | IllegalArgumentException | InvocationTargetException e) {
      // continue
    }

    if (classServerUri == null) {
      try {
        Method classServer = interpreter.intp().getClass().getMethod("classServerUri");
        classServerUri = (String) classServer.invoke(interpreter.intp());
      }
      catch (NoSuchMethodException | SecurityException | IllegalAccessException
          | IllegalArgumentException | InvocationTargetException e) {
        // continue instead of: throw new InterpreterException(e);
        // Newer Spark versions (like the patched CDH5.7.0 one) don't contain this method
       // logger.warn(String.format("Spark method classServerUri not available due to: [%s]",
         //   e.getMessage()));
      }
      props.put("spark.repl.class.uri", classServerUri);

    }


    return props;

  }
}
