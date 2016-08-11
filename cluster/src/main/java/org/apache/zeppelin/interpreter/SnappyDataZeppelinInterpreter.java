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
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import scala.collection.Seq;
import scala.collection.convert.WrapAsJava$;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.repl.SparkCommandLine;
import org.apache.spark.repl.SparkILoop;
import org.apache.spark.repl.SparkIMain;
import org.apache.spark.repl.SparkJLineCompletion;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.DAGScheduler;
import org.apache.spark.scheduler.Pool;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SnappyContext;
import org.apache.spark.ui.jobs.JobProgressListener;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.spark.SparkOutputStream;
import org.apache.zeppelin.spark.SparkVersion;
import org.apache.zeppelin.spark.ZeppelinContext;
import org.apache.zeppelin.spark.dep.SparkDependencyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Console;
import scala.Enumeration.Value;
import scala.None;
import scala.Some;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.mutable.HashMap;
import scala.collection.mutable.HashSet;
import scala.reflect.io.AbstractFile;
import scala.tools.nsc.Settings;
import scala.tools.nsc.interpreter.Completion.Candidates;
import scala.tools.nsc.interpreter.Completion.ScalaCompleter;
import scala.tools.nsc.settings.MutableSettings;
import scala.tools.nsc.settings.MutableSettings.BooleanSetting;
import scala.tools.nsc.settings.MutableSettings.PathSetting;
import org.apache.zeppelin.interpreter.Interpreter.FormType;


/**
 * Most of the contents of this class is borrowed from Spark Interpreter in zeppelin
 * https://github.com/apache/zeppelin/blob/master/spark/src/main/java/org/apache/zeppelin/spark/SparkInterpreter.java
 */
public class SnappyDataZeppelinInterpreter extends Interpreter {
  public static Logger logger = LoggerFactory.getLogger(SnappyDataZeppelinInterpreter.class);

  private ZeppelinContext z;
  private static SparkILoop interpreter;
  private static SparkIMain intp;
  private static SparkContext sc;
  private static SQLContext sqlc;
  private static SparkEnv env;
  private static JobProgressListener sparkListener;
  private static AbstractFile classOutputDir;
  private static Integer sharedInterpreterLock = new Integer(0);
  private static AtomicInteger numReferenceOfSparkContext = new AtomicInteger(0);

  private static SparkOutputStream out;
  private SparkDependencyResolver dep;
  private SparkJLineCompletion completor;

  private Map<String, Object> binder;
  private SparkVersion sparkVersion;
  private Settings settings = new Settings();

  public SnappyDataZeppelinInterpreter(Properties property) {
    super(property);

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

  private boolean useHiveContext() {
    return Boolean.parseBoolean(getProperty("zeppelin.spark.useHiveContext"));
  }

  public SQLContext getSQLContext() {
    if (SnappyContext.globalSparkContext() != null) {
      return new SnappyContext(SnappyContext.globalSparkContext());
    }else{
      return new SnappyContext(sc);
    }
  }


  public SparkDependencyResolver getDependencyResolver() {
    if (dep == null) {
      dep = new SparkDependencyResolver(intp,
          sc,
          getProperty("zeppelin.dep.localrepo"),
          getProperty("zeppelin.dep.additionalRemoteRepository"));
    }
    return dep;
  }

  public SparkContext createSparkContext() {
    logger.info("------ Create new SparkContext {} -------", getProperty("master"));

    if (null != interpreter && null != SnappyContext.globalSparkContext()) {
      return SnappyContext.globalSparkContext();
    }else{
      Properties props=ZeppelinIntpUtil.initializeZeppelinReplAndGetConfig();

      String[] jars = SparkILoop.getAddedJars();
      SparkConf conf =
          new SparkConf()
              .setMaster(getProperty("master"))
              .setAppName(getProperty("spark.app.name"));
      conf.set("spark.scheduler.mode", "FAIR");

      if (jars.length > 0) {
        conf.setJars(jars);
      }
      for(Map.Entry entry:props.entrySet()){
        conf.set(entry.getKey().toString(),entry.getValue().toString());
      }
      return new SparkContext(conf);
    }
  }

  static final String toString(Object o) {
    return (o instanceof String) ? (String)o : "";
  }

  private boolean useSparkSubmit() {
    return null != System.getenv("SPARK_SUBMIT");
  }

  public static String getSystemDefault(
      String envName,
      String propertyName,
      String defaultValue) {

    if (envName != null && !envName.isEmpty()) {
      String envValue = System.getenv().get(envName);
      if (envValue != null) {
        return envValue;
      }
    }

    if (propertyName != null && !propertyName.isEmpty()) {
      String propValue = System.getProperty(propertyName);
      if (propValue != null) {
        return propValue;
      }
    }
    return defaultValue;
  }

  @Override
  public void open() {

    synchronized (sharedInterpreterLock) {
      sc = getSparkContext();

      if (sc.getPoolForName("fair").isEmpty()) {
        Value schedulingMode = org.apache.spark.scheduler.SchedulingMode.FAIR();
        int minimumShare = 0;
        int weight = 1;
        Pool pool = new Pool("fair", schedulingMode, minimumShare, weight);
        sc.taskScheduler().rootPool().addSchedulable(pool);
      }

      sparkVersion = SparkVersion.fromVersionString(sc.version());

      sqlc = getSQLContext();

      dep = getDependencyResolver();

      z = new ZeppelinContext(sc, sqlc, null, dep,
          Integer.parseInt(getProperty("zeppelin.spark.maxResult")));

      intp.interpret("@transient var _binder = new java.util.HashMap[String, Object]()");
      binder = (Map<String, Object>)getValue("_binder");
      binder.put("sc", sc);
      binder.put("snc", sqlc);
      binder.put("z", z);

      intp.interpret("@transient val z = "
          + "_binder.get(\"z\").asInstanceOf[org.apache.zeppelin.spark.ZeppelinContext]");
      intp.interpret("@transient val sc = "
          + "_binder.get(\"sc\").asInstanceOf[org.apache.spark.SparkContext]");
      intp.interpret("@transient val snc = "
          + "_binder.get(\"snc\").asInstanceOf[org.apache.spark.sql.SnappyContext]");
      intp.interpret("@transient val sqlContext = "
          + "_binder.get(\"snc\").asInstanceOf[org.apache.spark.sql.SnappyContext]");
      intp.interpret("import org.apache.spark.SparkContext._");
      intp.interpret("import org.apache.spark.sql.SnappyContext._");
      if (sparkVersion.oldSqlContextImplicits()) {
        intp.interpret("import sqlContext._");
      } else {
        intp.interpret("import sqlContext.implicits._");
        intp.interpret("import sqlContext.sql");
        intp.interpret("import org.apache.spark.sql.functions._");
      }

    }

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
    if (buf.length() < cursor) {
      cursor = buf.length();
    }
    String completionText = getCompletionTargetString(buf, cursor);
    if (completionText == null) {
      completionText = "";
      cursor = completionText.length();
    }
    ScalaCompleter c = completor.completer();
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

  public Object getValue(String name) {
    Object ret = intp.valueOfTerm(name);
    if (ret instanceof None) {
      return null;
    } else if (ret instanceof Some) {
      return ((Some)ret).get();
    } else {
      return ret;
    }
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


  //This method has snappydata related changes
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
        res = intp.interpret(incomplete + s);
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
      Set<Tuple2<Object, Object>> keys =
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

  /*
  Override and keep close method empty to avoid closing spark context
   */
  @Override
  public void close() {

  }

  /**
   *
   * @return
   */
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

  public SparkVersion getSparkVersion() {
    return sparkVersion;
  }

  //This method has snappydata related changes
  private void initializeReplInterpreter() {
    out = new SparkOutputStream();
    URL[] urls = getClassloaderUrls();

    Settings settings = new Settings();
    if (getProperty("args") != null) {
      String[] argsArray = getProperty("args").split(" ");
      LinkedList<String> argList = new LinkedList<String>();
      for (String arg : argsArray) {
        argList.add(arg);
      }

      SparkCommandLine command =
          new SparkCommandLine(JavaConversions.asScalaBuffer(
              argList).toList());
      settings = command.settings();
    }

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

    System.setProperty("scala.repl.name.line", "line" + this.hashCode() + "$");

    // To prevent 'File name too long' error on some file system.
    MutableSettings.IntSetting numClassFileSetting = settings.maxClassfileName();
    numClassFileSetting.v_$eq(128);
    settings.scala$tools$nsc$settings$ScalaSettings$_setter_$maxClassfileName_$eq(
        numClassFileSetting);


    synchronized (sharedInterpreterLock) {
      /* create scala repl */
      this.interpreter = new SparkILoop(null, new PrintWriter(out));
      interpreter.settings_$eq(settings);

      interpreter.createInterpreter();

      intp = interpreter.intp();
      intp.setContextClassLoader();
      intp.initializeSynchronous();

      if (classOutputDir == null) {
        classOutputDir = settings.outputDirs().getSingleOutput().get();
      } else {
        // change SparkIMain class output dir
        settings.outputDirs().setSingleOutput(classOutputDir);
        ClassLoader cl = intp.classLoader();

        try {
          Field rootField = cl.getClass().getSuperclass().getDeclaredField("root");
          rootField.setAccessible(true);
          rootField.set(cl, classOutputDir);
        } catch (NoSuchFieldException | IllegalAccessException e) {
          logger.error(e.getMessage(), e);
        }
      }

      completor = new SparkJLineCompletion(intp);
    }
  }

  /**
   * This method is introduced for snappydata
   */
  public SparkILoop getReplInterpreter() {
    if (interpreter != null) {
      return interpreter;
    } else {
      initializeReplInterpreter();
      return interpreter;
    }
  }
}
