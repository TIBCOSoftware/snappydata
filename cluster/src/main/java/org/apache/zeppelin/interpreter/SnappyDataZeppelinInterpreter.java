/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package org.apache.zeppelin.interpreter;


import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;

import org.apache.spark.SecurityManager;
import org.apache.spark.repl.SparkILoop;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.DAGScheduler;
import org.apache.spark.scheduler.Pool;
import org.apache.spark.sql.SnappyContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ui.jobs.JobProgressListener;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterUtils;
import org.apache.zeppelin.interpreter.WrappedInterpreter;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.spark.SparkOutputStream;
import org.apache.zeppelin.spark.SparkVersion;
import org.apache.zeppelin.spark.ZeppelinContext;
import org.apache.zeppelin.spark.dep.SparkDependencyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.*;
import scala.Enumeration.Value;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.convert.WrapAsJava$;
import scala.collection.mutable.HashMap;
import scala.collection.mutable.HashSet;
import scala.reflect.io.AbstractFile;
import scala.tools.nsc.Global;
import scala.tools.nsc.Settings;
import scala.tools.nsc.interpreter.Completion.Candidates;
import scala.tools.nsc.interpreter.Completion.ScalaCompleter;
import scala.tools.nsc.interpreter.IMain;
import scala.tools.nsc.interpreter.Results;
import scala.tools.nsc.settings.MutableSettings;
import scala.tools.nsc.settings.MutableSettings.BooleanSetting;
import scala.tools.nsc.settings.MutableSettings.PathSetting;

import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.interpreter.Interpreter.FormType;

/**
 * Most of the contents of this class is borrowed from Spark Interpreter in zeppelin
 * https://github.com/apache/zeppelin/blob/master/spark/src/main/java/org/apache/zeppelin/spark/SparkInterpreter.java
 */
public class SnappyDataZeppelinInterpreter extends Interpreter {
  public static Logger logger = LoggerFactory.getLogger(SnappyDataZeppelinInterpreter.class);
  private ZeppelinContext z;
  private SparkILoop interpreter;
  /**
   * intp - scala.tools.nsc.interpreter.IMain; (scala 2.11)
   */
  private Object intp;
  private SparkConf conf;
  private static SparkContext sc;
  private static SnappyContext snc;
  private static SparkEnv env;
  private static SparkSession sparkSession;    // spark 2.x
  private static JobProgressListener sparkListener;
  private static Integer sharedInterpreterLock = new Integer(0);


  private SparkOutputStream out;
  private SparkDependencyResolver dep;

  /**
   * completer - org.apache.spark.repl.SparkJLineCompletion (scala 2.10)
   */
  private Object completer = null;
  private Map<String, Object> binder;
  private SparkVersion sparkVersion;
  private File outputDir;          // class outputdir for scala 2.11


  public SnappyDataZeppelinInterpreter(Properties property) {
    super(property);
    out = new SparkOutputStream(logger);
  }

  public SnappyDataZeppelinInterpreter(Properties property, SparkContext sc) {
    this(property);

    this.sc = sc;
    env = SparkEnv.get();
    sparkListener = setupListeners(this.sc);
  }

  public SparkContext getSparkContext() {
    synchronized (sharedInterpreterLock) {
      if (sc == null) {
        sc = createSparkContext();
        env = SparkEnv.get();
        sparkListener = setupListeners(sc);
      }
      return sc;
    }
  }

  public boolean isSparkContextInitialized() {
    synchronized (sharedInterpreterLock) {
      return sc != null;
    }
  }

  static JobProgressListener setupListeners(SparkContext context) {
    JobProgressListener pl = new JobProgressListener(context.getConf());
    try {
      Object listenerBus = context.getClass().getMethod("listenerBus").invoke(context);

      Method[] methods = listenerBus.getClass().getMethods();
      Method addListenerMethod = null;
      for (Method m : methods) {
        if (!m.getName().equals("addListener")) {
          continue;
        }

        Class<?>[] parameterTypes = m.getParameterTypes();

        if (parameterTypes.length != 1) {
          continue;
        }

        if (!parameterTypes[0].isAssignableFrom(JobProgressListener.class)) {
          continue;
        }

        addListenerMethod = m;
        break;
      }

      if (addListenerMethod != null) {
        addListenerMethod.invoke(listenerBus, pl);
      } else {
        return null;
      }
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException
        | IllegalArgumentException | InvocationTargetException e) {
      logger.error(e.toString(), e);
      return null;
    }
    return pl;
  }

  private boolean importImplicit() {
    return java.lang.Boolean.parseBoolean(getProperty("zeppelin.spark.importImplicit"));
  }

  public SparkSession getSparkSession() {
    synchronized (sharedInterpreterLock) {
      if (sparkSession == null) {
        sparkSession = createSparkSession();
      }
      return sparkSession;
    }
  }

  public SnappyContext getSnappyContext() {
    synchronized (sharedInterpreterLock) {
      SnappyContext snc = new SnappyContext(getSparkContext());
      return snc;
    }
  }


  public SparkDependencyResolver getDependencyResolver() {
    if (dep == null) {
      dep = new SparkDependencyResolver(
          (Global)ZeppelinIntpUtil.invokeMethod(intp, "global"),
          (ClassLoader)ZeppelinIntpUtil.invokeMethod(ZeppelinIntpUtil.invokeMethod(intp, "classLoader"), "getParent"),
          sc,
          getProperty("zeppelin.dep.localrepo"),
          getProperty("zeppelin.dep.additionalRemoteRepository"));
    }
    return dep;
  }


  /**
   * Spark 2.x
   * Create SparkSession
   */
  public SparkSession createSparkSession() {
    logger.info("------ Create new SparkSession {} -------", getProperty("master"));

    if (null != SnappyContext.globalSparkContext()) {
      SparkSession.Builder sessionBuilder = SparkSession.builder().sparkContext(SnappyContext.globalSparkContext());
      SparkSession session = sessionBuilder.getOrCreate();
      return session;
    } else {
      //This is needed is user is using Snappydata interpreter without installing cluster
      String execUri = System.getenv("SPARK_EXECUTOR_URI");
      conf.setAppName(getProperty("spark.app.name"));

      conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath());

      if (execUri != null) {
        conf.set("spark.executor.uri", execUri);
      }

      if (System.getenv("SPARK_HOME") != null) {
        conf.setSparkHome(System.getenv("SPARK_HOME"));
      }

      conf.set("spark.scheduler.mode", "FAIR");
      conf.setMaster(getProperty("master"));

      Properties intpProperty = getProperty();

      for (Object k : intpProperty.keySet()) {
        String key = (String)k;
        String val = toString(intpProperty.get(key));
        if (!key.startsWith("spark.") || !val.trim().isEmpty()) {
          logger.debug(String.format("SparkConf: key = [%s], value = [%s]", key, val));
          conf.set(key, val);
        }
      }

      //Class SparkSession = ZeppelinIntpUtil.findClass("org.apache.spark.sql.SparkSession");
      SparkSession.Builder builder = SparkSession.builder();
      builder.config(conf);

      sparkSession = builder.getOrCreate();

      return sparkSession;

    }

  }

  public SparkContext createSparkContext() {
    return sparkSession.sparkContext();

  }


  static final String toString(Object o) {
    return (o instanceof String) ? (String)o : "";
  }


  @Override
  public void open() {

    // set properties and do login before creating any spark stuff for secured cluster
    if (getProperty("master").equals("yarn-client")) {
      System.setProperty("SPARK_YARN_MODE", "true");
    }
    if (getProperty().containsKey("spark.yarn.keytab") &&
        getProperty().containsKey("spark.yarn.principal")) {
      try {
        String keytab = getProperty().getProperty("spark.yarn.keytab");
        String principal = getProperty().getProperty("spark.yarn.principal");
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
      } catch (IOException e) {
        throw new RuntimeException("Can not pass kerberos authentication", e);
      }
    }

    conf = new SparkConf();
    URL[] urls = getClassloaderUrls();

    // Very nice discussion about how scala compiler handle classpath
    // https://groups.google.com/forum/#!topic/scala-user/MlVwo2xCCI0

    /*
     * > val env = new nsc.Settings(errLogger) > env.usejavacp.value = true > val p = new
     * Interpreter(env) > p.setContextClassLoader > Alternatively you can set the class path through
     * nsc.Settings.classpath.
     *
     * >> val settings = new Settings() >> settings.usejavacp.value = true >>
     * settings.classpath.value += File.pathSeparator + >> System.getProperty("java.class.path") >>
     * val in = new Interpreter(settings) { >> override protected def parentClassLoader =
     * getClass.getClassLoader >> } >> in.setContextClassLoader()
     */
    Settings settings = new Settings();

    // process args
    String args = getProperty("args");
    if (args == null) {
      args = "";
    }

    String[] argsArray = args.split(" ");
    LinkedList<String> argList = new LinkedList<String>();
    for (String arg : argsArray) {
      argList.add(arg);
    }

    String sparkReplClassDir = getProperty("spark.repl.classdir");
    if (sparkReplClassDir == null) {
      sparkReplClassDir = System.getProperty("spark.repl.classdir");
    }
    if (sparkReplClassDir == null) {
      sparkReplClassDir = System.getProperty("java.io.tmpdir");
    }

    outputDir = createTempDir(sparkReplClassDir);

    argList.add("-Yrepl-class-based");
    argList.add("-Yrepl-outdir");
    argList.add(outputDir.getAbsolutePath());


    scala.collection.immutable.List<String> list =
        JavaConversions.asScalaBuffer(argList).toList();

    settings.processArguments(list, true);


    // set classpath for scala compiler
    PathSetting pathSettings = settings.classpath();
    String classpath = "";
    List<File> paths = currentClassPath();
    for (File f : paths) {
      if (classpath.length() > 0) {
        classpath += File.pathSeparator;
      }
      classpath += f.getAbsolutePath();
    }

    if (urls != null) {
      for (URL u : urls) {
        if (classpath.length() > 0) {
          classpath += File.pathSeparator;
        }
        classpath += u.getFile();
      }
    }

    // add dependency from local repo
    String localRepo = getProperty("zeppelin.interpreter.localRepo");
    if (localRepo != null) {
      File localRepoDir = new File(localRepo);
      if (localRepoDir.exists()) {
        File[] files = localRepoDir.listFiles();
        if (files != null) {
          for (File f : files) {
            if (classpath.length() > 0) {
              classpath += File.pathSeparator;
            }
            classpath += f.getAbsolutePath();
          }
        }
      }
    }

    pathSettings.v_$eq(classpath);
    settings.scala$tools$nsc$settings$ScalaSettings$_setter_$classpath_$eq(pathSettings);


    // set classloader for scala compiler
    settings.explicitParentLoader_$eq(new Some<ClassLoader>(Thread.currentThread()
        .getContextClassLoader()));
    BooleanSetting b = (BooleanSetting)settings.usejavacp();
    b.v_$eq(true);
    settings.scala$tools$nsc$settings$StandardScalaSettings$_setter_$usejavacp_$eq(b);

    /* Required for scoped mode.
     * In scoped mode multiple scala compiler (repl) generates class in the same directory.
     * Class names is not randomly generated and look like '$line12.$read$$iw$$iw'
     * Therefore it's possible to generated class conflict(overwrite) with other repl generated
     * class.
     *
     * To prevent generated class name conflict,
     * change prefix of generated class name from each scala compiler (repl) instance.
     *
     * In Spark 2.x, REPL generated wrapper class name should compatible with the pattern
     * ^(\$line(?:\d+)\.\$read)(?:\$\$iw)+$
     */
    System.setProperty("scala.repl.name.line", "$line" + this.hashCode());

    // To prevent 'File name too long' error on some file system.
    MutableSettings.IntSetting numClassFileSetting = settings.maxClassfileName();
    numClassFileSetting.v_$eq(128);
    settings.scala$tools$nsc$settings$ScalaSettings$_setter_$maxClassfileName_$eq(
        numClassFileSetting);

    synchronized (sharedInterpreterLock) {
      /* create scala repl */

      this.interpreter = new SparkILoop((java.io.BufferedReader)null, new PrintWriter(out));

      interpreter.settings_$eq(settings);

      interpreter.createInterpreter();

      intp = ZeppelinIntpUtil.invokeMethod(interpreter, "intp");
      ZeppelinIntpUtil.invokeMethod(intp, "setContextClassLoader");
      ZeppelinIntpUtil.invokeMethod(intp, "initializeSynchronous");


      if (ZeppelinIntpUtil.findClass("org.apache.spark.repl.SparkJLineCompletion", true) != null) {
        completer = ZeppelinIntpUtil.instantiateClass(
            "org.apache.spark.repl.SparkJLineCompletion",
            new Class[]{ZeppelinIntpUtil.findClass("org.apache.spark.repl.SparkIMain")},
            new Object[]{intp});
      } else if (ZeppelinIntpUtil.findClass(
          "scala.tools.nsc.interpreter.PresentationCompilerCompleter", true) != null) {
        completer = ZeppelinIntpUtil.instantiateClass(
            "scala.tools.nsc.interpreter.PresentationCompilerCompleter",
            new Class[]{IMain.class},
            new Object[]{intp});
      } else if (ZeppelinIntpUtil.findClass(
          "scala.tools.nsc.interpreter.JLineCompletion", true) != null) {
        completer = ZeppelinIntpUtil.instantiateClass(
            "scala.tools.nsc.interpreter.JLineCompletion",
            new Class[]{IMain.class},
            new Object[]{intp});
      }
      sparkSession = getSparkSession();
      sc = getSparkContext();
      if (sc.getPoolForName("fair").isEmpty()) {
        Value schedulingMode = org.apache.spark.scheduler.SchedulingMode.FAIR();
        int minimumShare = 0;
        int weight = 1;
        Pool pool = new Pool("fair", schedulingMode, minimumShare, weight);
        sc.taskScheduler().rootPool().addSchedulable(pool);
      }

      sparkVersion = SparkVersion.fromVersionString(sc.version());

      snc = getSnappyContext();

      dep = getDependencyResolver();

      z = new ZeppelinContext(sc, snc, null, dep,
          Integer.parseInt(getProperty("zeppelin.spark.maxResult")));

      interpret("@transient val _binder = new java.util.HashMap[String, Object]()");
      Map<String, Object> binder;

      binder = (Map<String, Object>)getLastObject();
      binder.put("sc", sc);
      binder.put("snc", snc);
      binder.put("z", z);

      binder.put("spark", sparkSession);


      interpret("@transient val z = "
          + "_binder.get(\"z\").asInstanceOf[org.apache.zeppelin.spark.ZeppelinContext]");
      interpret("@transient val sc = "
          + "_binder.get(\"sc\").asInstanceOf[org.apache.spark.SparkContext]");
      // Injecting SnappyContext in repl
      interpret("@transient val snc = "
          + "_binder.get(\"snc\").asInstanceOf[org.apache.spark.sql.SnappyContext]");
      interpret("@transient val snappyContext = "
          + "_binder.get(\"snc\").asInstanceOf[org.apache.spark.sql.SnappyContext]");

      interpret("@transient val spark = "
          + "_binder.get(\"spark\").asInstanceOf[org.apache.spark.sql.SparkSession]");

      interpret("import org.apache.spark.SparkContext._");
      interpret("import org.apache.spark.sql.SnappyContext._");
      interpret("import org.apache.spark.sql.{Row, SaveMode, SnappyContext}");
      if (importImplicit()) {
        interpret("import spark.implicits._");
        interpret("import spark.sql");
        interpret("import org.apache.spark.sql.functions._");

      }
    }

    /* Temporary disabling DisplayUtils. see https://issues.apache.org/jira/browse/ZEPPELIN-127
     *
    // Utility functions for display
    intp.interpret("import org.apache.zeppelin.spark.utils.DisplayUtils._");

    // Scala implicit value for spark.maxResult
    intp.interpret("import org.apache.zeppelin.spark.utils.SparkMaxResult");
    intp.interpret("implicit val sparkMaxResult = new SparkMaxResult(" +
            Integer.parseInt(getProperty("zeppelin.spark.maxResult")) + ")");
     */


    // add jar from local repo
    if (localRepo != null) {
      File localRepoDir = new File(localRepo);
      if (localRepoDir.exists()) {
        File[] files = localRepoDir.listFiles();
        if (files != null) {
          for (File f : files) {
            if (f.getName().toLowerCase().endsWith(".jar")) {
              sc.addJar(f.getAbsolutePath());
              logger.info("sc.addJar(" + f.getAbsolutePath() + ")");
            } else {
              sc.addFile(f.getAbsolutePath());
              logger.info("sc.addFile(" + f.getAbsolutePath() + ")");
            }
          }
        }
      }
    }

  }

  private Results.Result interpret(String line) {
    return (Results.Result)ZeppelinIntpUtil.invokeMethod(
        intp,
        "interpret",
        new Class[]{String.class},
        new Object[]{line});
  }

  private List<File> currentClassPath() {
    List<File> paths = classPath(Thread.currentThread().getContextClassLoader());
    String[] cps = System.getProperty("java.class.path").split(File.pathSeparator);
    if (cps != null) {
      for (String cp : cps) {
        paths.add(new File(cp));
      }
    }
    return paths;
  }

  private List<File> classPath(ClassLoader cl) {
    List<File> paths = new LinkedList<File>();
    if (cl == null) {
      return paths;
    }

    if (cl instanceof URLClassLoader) {
      URLClassLoader ucl = (URLClassLoader)cl;
      URL[] urls = ucl.getURLs();
      if (urls != null) {
        for (URL url : urls) {
          paths.add(new File(url.getFile()));
        }
      }
    }
    return paths;
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor) {
    if (completer == null) {
      logger.warn("Can't find completer");
      return new LinkedList<InterpreterCompletion>();
    }

    if (buf.length() < cursor) {
      cursor = buf.length();
    }
    String completionText = getCompletionTargetString(buf, cursor);
    if (completionText == null) {
      completionText = "";
      cursor = completionText.length();
    }

    ScalaCompleter c = (ScalaCompleter)ZeppelinIntpUtil.invokeMethod(completer, "completer");
    Candidates ret = c.complete(completionText, cursor);

    List<String> candidates = WrapAsJava$.MODULE$.seqAsJavaList(ret.candidates());
    List<InterpreterCompletion> completions = new LinkedList<InterpreterCompletion>();

    for (String candidate : candidates) {
      completions.add(new InterpreterCompletion(candidate, candidate));
    }

    return completions;
  }

  private String getCompletionTargetString(String text, int cursor) {
    String[] completionSeqCharaters = {" ", "\n", "\t"};
    int completionEndPosition = cursor;
    int completionStartPosition = cursor;
    int indexOfReverseSeqPostion = cursor;

    String resultCompletionText = "";
    String completionScriptText = "";
    try {
      completionScriptText = text.substring(0, cursor);
    } catch (Exception e) {
      logger.error(e.toString());
      return null;
    }
    completionEndPosition = completionScriptText.length();

    String tempReverseCompletionText = new StringBuilder(completionScriptText).reverse().toString();

    for (String seqCharacter : completionSeqCharaters) {
      indexOfReverseSeqPostion = tempReverseCompletionText.indexOf(seqCharacter);

      if (indexOfReverseSeqPostion < completionStartPosition && indexOfReverseSeqPostion > 0) {
        completionStartPosition = indexOfReverseSeqPostion;
      }

    }

    if (completionStartPosition == completionEndPosition) {
      completionStartPosition = 0;
    } else {
      completionStartPosition = completionEndPosition - completionStartPosition;
    }
    resultCompletionText = completionScriptText.substring(
        completionStartPosition, completionEndPosition);

    return resultCompletionText;
  }


  public Object getLastObject() {
    IMain.Request r = (IMain.Request)ZeppelinIntpUtil.invokeMethod(intp, "lastRequest");
    if (r == null || r.lineRep() == null) {
      return null;
    }
    Object obj = r.lineRep().call("$result",
        JavaConversions.asScalaBuffer(new LinkedList<Object>()));
    return obj;
  }

  String getJobGroup(InterpreterContext context) {
    return "zeppelin-" + context.getParagraphId();
  }

  /**
   * Interpret a single line.
   */
  @Override
  public InterpreterResult interpret(String line, InterpreterContext context) {
    if (sparkVersion.isUnsupportedVersion()) {
      return new InterpreterResult(Code.ERROR, "Spark " + sparkVersion.toString()
          + " is not supported");
    }

    z.setInterpreterContext(context);
    if (line == null || line.trim().length() == 0) {
      return new InterpreterResult(Code.SUCCESS);
    }
    return interpret(line.split("\n"), context);
  }

  public InterpreterResult interpret(String[] lines, InterpreterContext context) {
    synchronized (this) {
      z.setGui(context.getGui());
      sc.setJobGroup(getJobGroup(context), "Zeppelin", false);
      InterpreterResult r = interpretInput(lines, context);
      sc.clearJobGroup();
      return r;
    }
  }

  public InterpreterResult interpretInput(String[] lines, InterpreterContext context) {
    SparkEnv.set(env);

    // add print("") to make sure not finishing with comment
    // see https://github.com/NFLabs/zeppelin/issues/151
    String[] linesToRun = new String[lines.length + 1];
    for (int i = 0; i < lines.length; i++) {
      linesToRun[i] = lines[i];
    }
    linesToRun[lines.length] = "print(\"\")";

    Console.setOut(context.out);
    out.setInterpreterOutput(context.out);
    context.out.clear();
    Code r = null;
    String incomplete = "";
    boolean inComment = false;

    for (int l = 0; l < linesToRun.length; l++) {
      String s = linesToRun[l];
      // check if next line starts with "." (but not ".." or "./") it is treated as an invocation
      if (l + 1 < linesToRun.length) {
        String nextLine = linesToRun[l + 1].trim();
        boolean continuation = false;
        if (nextLine.isEmpty()
            || nextLine.startsWith("//")         // skip empty line or comment
            || nextLine.startsWith("}")
            || nextLine.startsWith("object")) {  // include "} object" for Scala companion object
          continuation = true;
        } else if (!inComment && nextLine.startsWith("/*")) {
          inComment = true;
          continuation = true;
        } else if (inComment && nextLine.lastIndexOf("*/") >= 0) {
          inComment = false;
          continuation = true;
        } else if (nextLine.length() > 1
            && nextLine.charAt(0) == '.'
            && nextLine.charAt(1) != '.'     // ".."
            && nextLine.charAt(1) != '/') {  // "./"
          continuation = true;
        } else if (inComment) {
          continuation = true;
        }
        if (continuation) {
          incomplete += s + "\n";
          continue;
        }
      }

      scala.tools.nsc.interpreter.Results.Result res = null;
      try {
        res = interpret(incomplete + s);
      } catch (Exception e) {
        sc.clearJobGroup();
        out.setInterpreterOutput(null);
        logger.info("Interpreter exception", e);
        return new InterpreterResult(Code.ERROR, InterpreterUtils.getMostRelevantMessage(e));
      }

      r = getResultCode(res);

      if (r == Code.ERROR) {
        sc.clearJobGroup();
        out.setInterpreterOutput(null);
        return new InterpreterResult(r, "");
      } else if (r == Code.INCOMPLETE) {
        incomplete += s + "\n";
      } else {
        incomplete = "";
      }
    }

    // make sure code does not finish with comment
    if (r == Code.INCOMPLETE) {
      scala.tools.nsc.interpreter.Results.Result res = null;
      res = interpret(incomplete + "\nprint(\"\")");
      r = getResultCode(res);
    }

    if (r == Code.INCOMPLETE) {
      sc.clearJobGroup();
      out.setInterpreterOutput(null);
      return new InterpreterResult(r, "Incomplete expression");
    } else {
      sc.clearJobGroup();
      out.setInterpreterOutput(null);
      return new InterpreterResult(Code.SUCCESS);
    }
  }

  @Override
  public void cancel(InterpreterContext context) {
    sc.cancelJobGroup(getJobGroup(context));
  }

  @Override
  public int getProgress(InterpreterContext context) {
    String jobGroup = getJobGroup(context);
    int completedTasks = 0;
    int totalTasks = 0;

    DAGScheduler scheduler = sc.dagScheduler();
    if (scheduler == null) {
      return 0;
    }
    HashSet<ActiveJob> jobs = scheduler.activeJobs();
    if (jobs == null || jobs.size() == 0) {
      return 0;
    }
    Iterator<ActiveJob> it = jobs.iterator();
    while (it.hasNext()) {
      ActiveJob job = it.next();
      String g = (String)job.properties().get("spark.jobGroup.id");
      if (jobGroup.equals(g)) {
        int[] progressInfo = null;
        try {
          Object finalStage = job.getClass().getMethod("finalStage").invoke(job);
          if (sparkVersion.getProgress1_0()) {
            progressInfo = getProgressFromStage_1_0x(sparkListener, finalStage);
          } else {
            progressInfo = getProgressFromStage_1_1x(sparkListener, finalStage);
          }
        } catch (IllegalAccessException | IllegalArgumentException
            | InvocationTargetException | NoSuchMethodException
            | SecurityException e) {
          logger.error("Can't get progress info", e);
          return 0;
        }
        totalTasks += progressInfo[0];
        completedTasks += progressInfo[1];
      }
    }

    if (totalTasks == 0) {
      return 0;
    }
    return completedTasks * 100 / totalTasks;
  }


  private int[] getProgressFromStage_1_0x(JobProgressListener sparkListener, Object stage)
      throws IllegalAccessException, IllegalArgumentException,
      InvocationTargetException, NoSuchMethodException, SecurityException {
    int numTasks = (int)stage.getClass().getMethod("numTasks").invoke(stage);
    int completedTasks = 0;

    int id = (int)stage.getClass().getMethod("id").invoke(stage);

    Object completedTaskInfo = null;

    completedTaskInfo = JavaConversions.mapAsJavaMap(
        (HashMap<Object, Object>)sparkListener.getClass()
            .getMethod("stageIdToTasksComplete").invoke(sparkListener)).get(id);

    if (completedTaskInfo != null) {
      completedTasks += (int)completedTaskInfo;
    }
    List<Object> parents = JavaConversions.seqAsJavaList((Seq<Object>)stage.getClass()
        .getMethod("parents").invoke(stage));
    if (parents != null) {
      for (Object s : parents) {
        int[] p = getProgressFromStage_1_0x(sparkListener, s);
        numTasks += p[0];
        completedTasks += p[1];
      }
    }

    return new int[]{numTasks, completedTasks};
  }

  private int[] getProgressFromStage_1_1x(JobProgressListener sparkListener, Object stage)
      throws IllegalAccessException, IllegalArgumentException,
      InvocationTargetException, NoSuchMethodException, SecurityException {
    int numTasks = (int)stage.getClass().getMethod("numTasks").invoke(stage);
    int completedTasks = 0;
    int id = (int)stage.getClass().getMethod("id").invoke(stage);

    try {
      Method stageIdToData = sparkListener.getClass().getMethod("stageIdToData");
      HashMap<Tuple2<Object, Object>, Object> stageIdData =
          (HashMap<Tuple2<Object, Object>, Object>)stageIdToData.invoke(sparkListener);
      Class<?> stageUIDataClass =
          this.getClass().forName("org.apache.spark.ui.jobs.UIData$StageUIData");

      Method numCompletedTasks = stageUIDataClass.getMethod("numCompleteTasks");
      java.util.Set<Tuple2<Object, Object>> keys =
          JavaConverters.setAsJavaSetConverter(stageIdData.keySet()).asJava();
      for (Tuple2<Object, Object> k : keys) {
        if (id == (int)k._1()) {
          Object uiData = stageIdData.get(k).get();
          completedTasks += (int)numCompletedTasks.invoke(uiData);
        }
      }
    } catch (Exception e) {
      logger.error("Error on getting progress information", e);
    }

    List<Object> parents = JavaConversions.seqAsJavaList((Seq<Object>)stage.getClass()
        .getMethod("parents").invoke(stage));
    if (parents != null) {
      for (Object s : parents) {
        int[] p = getProgressFromStage_1_1x(sparkListener, s);
        numTasks += p[0];
        completedTasks += p[1];
      }
    }
    return new int[]{numTasks, completedTasks};
  }

  private Code getResultCode(scala.tools.nsc.interpreter.Results.Result r) {
    if (r instanceof scala.tools.nsc.interpreter.Results.Success$) {
      return Code.SUCCESS;
    } else if (r instanceof scala.tools.nsc.interpreter.Results.Incomplete$) {
      return Code.INCOMPLETE;
    } else {
      return Code.ERROR;
    }
  }

  @Override
  public void close() {
    ZeppelinIntpUtil.invokeMethod(intp, "close");
  }

  @Override
  public FormType getFormType() {
    return FormType.NATIVE;
  }

  public JobProgressListener getJobProgressListener() {
    return sparkListener;
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetFIFOScheduler(
        SnappyDataZeppelinInterpreter.class.getName() + this.hashCode());
  }

  public ZeppelinContext getZeppelinContext() {
    return z;
  }


  private File createTempDir(String dir) {
    return ZeppelinIntpUtil.getTempDir();
  }


}
