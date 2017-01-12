/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package io.snappydata.hydra.installJar;

import hydra.FileUtil;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import org.apache.spark.SnappyTestUtils;
import scala.collection.JavaConversions;
import util.TestException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;


public class DynamicJarLoadingTest extends SnappyTest {

    protected static String getTempDir() {
        File log = new File(".");
        String dest = null;
        try {
            dest = log.getCanonicalPath() + File.separator + "temp";
        } catch (IOException e) {
            throw new TestException("IOException occurred while retriving destination temp dir " + log + "\nError Message:" + e.getMessage());
        }
        File tempDir = new File(dest);
        if (!tempDir.exists()) FileUtil.mkdir(tempDir);
        return tempDir.getAbsolutePath();
    }

    protected static String createJarFile(int numClasses, String classVersion) {
        String dir = getTempDir();
        List files = createClasses(numClasses, classVersion, dir);
        files.add(createPersonClass("Person", dir));
        files.add(createJobClass("DynamicJarLoadingJob", dir));
        return SnappyTestUtils.createJarFile((JavaConversions.asScalaBuffer(files)).toList(), dir);
    }

    protected static List<File> createClasses(int numClasses, String classVersion, String dir) {
        List files = new ArrayList();
        for (int i = 0; i <= numClasses; i++) {
            files.add(createSupportiveClasses("FakeClass" + i, classVersion, dir));
        }
        return files;
    }

    protected static String createJarWithOnlyClasses(int numClasses, String classVersion) {
        String dir = getTempDir();
        List files = createClasses(numClasses, classVersion, dir);
        return SnappyTestUtils.createJarFile((JavaConversions.asScalaBuffer(files)).toList(), dir);
    }

    protected static String createJarFileWithOnlyJobClass(String dir) {
        List files = new ArrayList();
        files.add(createPersonClass("Person", dir));
        files.add(createJobClass("DynamicJarLoadingJob", dir));
        return SnappyTestUtils.createJarFile((JavaConversions.asScalaBuffer(files)).toList(), dir);
    }

    protected static String createJarFileWithIdenticalJobClass(String dir) {
        List files = new ArrayList();
        files.add(createPersonClass("Person", dir));
        files.add(createJobClassWithDifferentLogStatements("DynamicJarLoadingJob", dir));
        return SnappyTestUtils.createJarFile((JavaConversions.asScalaBuffer(files)).toList(), dir);
    }

    public static void HydraTask_executeSnappyJobWithDynamicJarLoading_installJar() {
        String appJar = createJarFile(3, "1");
        executeSnappyJobWithDynamicJarLoading(appJar, "snappyJobInstallJarResult_thread_", null);
    }

    public static void HydraTask_executeSnappyJobWithDynamicJarLoading_modifyJar() {
        String appJar = createJarFile(2, "2");
        executeSnappyJobWithDynamicJarLoading(appJar, "snappyJobModifyJarResult_thread_", null);
    }

    protected static void executeSnappyJobWithDynamicJarLoading(String appJar, String logFileName, String appName) {
        int currentThread = snappyTest.getMyTid();
        String logFile = logFileName + currentThread + "_" + System.currentTimeMillis() + ".log";
        snappyTest.executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile, appJar, getTempDir(), appName);
    }

    public static File createSupportiveClasses(String className, String version, String destDir) {
        File dir = new File(destDir);
        String generalClasseText = "public class " + className + " implements java.io.Serializable {" +
                "  @Override public String toString() { return \"" + version + "\"; }}";
        return SnappyTestUtils.createCompiledClass(className,
                dir,
                SnappyTestUtils.getJavaSourceFromString(className, generalClasseText),
                new scala.collection.mutable.ArrayBuffer<URL>());
    }


    public static File createJobClass(String className, String destDir) {
        String generalClassText = "import com.typesafe.config.Config;\n" +
                "import org.apache.spark.sql.*;\n" +
                "\n" +
                "import java.io.File;\n" +
                "import java.io.FileOutputStream;\n" +
                "import java.io.PrintWriter;\n" +
                "import java.io.StringWriter;\n" +
                "import java.lang.reflect.Method;\n" +
                "\n" +
                "public class DynamicJarLoadingJob extends JavaSnappySQLJob {\n" +
                "    @Override\n" +
                "    public Object runSnappyJob(SnappySession spark, Config jobConfig) {\n" +
                "        PrintWriter pw = null;\n" +
                "        try {\n" +
                "            SnappyContext snc = spark.sqlContext();\n" +
                "            pw = new PrintWriter(new FileOutputStream(new File(jobConfig.getString(\"logFileName\")), true));\n" +
                "            Class c=Class.forName(\"person.Person\");\n" +
                "            Method m=c.getDeclaredMethod(\"displayName\", String.class);\n" +
                "            Object t = c.newInstance();\n" +
                "            m.invoke(t, \"Swati\");" +
                "            int numServers = Integer.parseInt(jobConfig.getString(\"numServers\"));\n" +
                "            boolean expectedException = Boolean.parseBoolean(jobConfig.getString(\"expectedException\"));\n" +
                "            pw.println(\"****** DynamicJarLoadingJob started ******\");\n" +
                "            String currentDirectory = new File(\".\").getCanonicalPath();\n" +
                "            io.snappydata.hydra.installJar.TestUtils.verify(snc, jobConfig.getString(\"classVersion\"), pw, numServers,expectedException);\n" +
                "            pw.println(\"****** DynamicJarLoadingJob finished ******\");" +
                "            return String.format(\"See %s/\" + jobConfig.getString(\"logFileName\"), currentDirectory);\n" +
                "        } catch (Exception e) {\n" +
                "            pw.println(\"ERROR: failed with \" + e.getMessage());\n" +
                "            e.printStackTrace(pw);\n" +
                "        } finally {\n" +
                "            pw.flush();\n" +
                "            pw.close();\n" +
                "        }\n" +
                "        return null;\n" +
                "    }" +
                "\n" +
                "    @Override\n" +
                "    public SnappyJobValidation isValidJob(SnappySession spark, Config jobConfig) {\n" +
                "        return new SnappyJobValid();\n" +
                "    }\n" +
                "}";
        return SnappyTestUtils.createCompiledClass(className,
                new File(destDir),
                SnappyTestUtils.getJavaSourceFromString(className, generalClassText),
                new scala.collection.mutable.ArrayBuffer<URL>());
    }

    public static File createPersonClass(String className, String destDir) {
        String dir = destDir + File.separator + "person";
        File person = new File(dir);
        if (!person.exists()) FileUtil.mkdir(person);
        String generalClassText = "package person;\n" +
                "import com.typesafe.config.Config;\n" +
                "public class Person {\n" +
                "    private String name;\n" +
                "\n" +
                "    public Person(String n) {\n" +
                "        name = n;\n" +
                "    }\n" +
                "\n" +
                "    public void displayName() {\n" +
                "        System.out.println(\"SS - Person Name is : \" + name);\n" +
                "    }\n" +
                "}";
        return SnappyTestUtils.createCompiledClass(className,
                person,
                SnappyTestUtils.getJavaSourceFromString(className, generalClassText),
                new scala.collection.mutable.ArrayBuffer<URL>());
    }

    public static File createJobClassWithDifferentLogStatements(String className, String destDir) {
        String generalClassText = "import com.typesafe.config.Config;\n" +
                "import org.apache.spark.sql.*;\n" +
                "\n" +
                "import java.io.File;\n" +
                "import java.io.FileOutputStream;\n" +
                "import java.io.PrintWriter;\n" +
                "import java.io.StringWriter;\n" +
                "import java.lang.reflect.Method;\n" +
                "\n" +
                "public class DynamicJarLoadingJob extends JavaSnappySQLJob {\n" +
                "    @Override\n" +
                "    public Object runSnappyJob(SnappySession spark, Config jobConfig) {\n" +
                "        PrintWriter pw = null;\n" +
                "        try {\n" +
                "            SnappyContext snc = spark.sqlContext();\n" +
                "            pw = new PrintWriter(new FileOutputStream(new File(jobConfig.getString(\"logFileName\")), true));\n" +
                "            Class c=Class.forName(\"person.Person\");\n" +
                "            Method m=c.getDeclaredMethod(\"displayName\", String.class);\n" +
                "            Object t = c.newInstance();\n" +
                "            m.invoke(t, \"Swati\");" +
                "            int numServers = Integer.parseInt(jobConfig.getString(\"numServers\"));\n" +
                "            boolean expectedException = Boolean.parseBoolean(jobConfig.getString(\"expectedException\"));\n" +
                "            pw.println(\"****** Started DynamicJarLoadingJob With having Identical name but different functionality ******\");\n" +
                "            String currentDirectory = new File(\".\").getCanonicalPath();\n" +
                "            io.snappydata.hydra.installJar.TestUtils.verify(snc, jobConfig.getString(\"classVersion\"), pw, numServers,expectedException);\n" +
                "            pw.println(\"****** Finished DynamicJarLoadingJob With having Identical name but different functionality ******\");" +
                "            return String.format(\"See %s/\" + jobConfig.getString(\"logFileName\"), currentDirectory);\n" +
                "        } catch (Exception e) {\n" +
                "            pw.println(\"ERROR: failed with \" + e.getMessage());\n" +
                "            e.printStackTrace(pw);\n" +
                "        } finally {\n" +
                "            pw.flush();\n" +
                "            pw.close();\n" +
                "        }\n" +
                "        return null;\n" +
                "    }" +
                "\n" +
                "    @Override\n" +
                "    public SnappyJobValidation isValidJob(SnappySession spark, Config jobConfig) {\n" +
                "        return new SnappyJobValid();\n" +
                "    }\n" +
                "}";
        return SnappyTestUtils.createCompiledClass(className,
                new File(destDir),
                SnappyTestUtils.getJavaSourceFromString(className, generalClassText),
                new scala.collection.mutable.ArrayBuffer<URL>());
    }

    public static File createJobAccessingClassFromPreviousJobExecution(String className, String destDir) {
        String generalClassText = "import com.typesafe.config.Config;\n" +
                "import org.apache.spark.sql.*;\n" +
                "\n" +
                "import java.io.File;\n" +
                "import java.io.FileOutputStream;\n" +
                "import java.io.PrintWriter;\n" +
                "import java.io.StringWriter;\n" +
                "import java.lang.reflect.Method;\n" +
                "\n" +
                "public class DynamicJarLoadingJob extends JavaSnappySQLJob {\n" +
                "    @Override\n" +
                "    public Object runSnappyJob(SnappySession spark, Config jobConfig) {\n" +
                "        PrintWriter pw = null;\n" +
                "        try {\n" +
                "            SnappyContext snc = spark.sqlContext();\n" +
                "            pw = new PrintWriter(new FileOutputStream(new File(jobConfig.getString(\"logFileName\")), true));\n" +
                "            Class c=Class.forName(\"person.Person\");\n" +
                "            Method m=c.getDeclaredMethod(\"displayName\", String.class);\n" +
                "            Object t = c.newInstance();\n" +
                "            m.invoke(t, \"Swati\");" +
                "            int numServers = Integer.parseInt(jobConfig.getString(\"numServers\"));\n" +
                "            boolean expectedException = Boolean.parseBoolean(jobConfig.getString(\"expectedException\"));\n" +
                "            pw.println(\"****** Started DynamicJarLoadingJob accessing class loaded through previous job execution ******\");\n" +
                "            String currentDirectory = new File(\".\").getCanonicalPath();\n" +
                "            io.snappydata.hydra.installJar.TestUtils.verifyClassFromPreviousJobExecution(snc, jobConfig.getString(\"classVersion\"), pw, numServers,expectedException);\n" +
                "            pw.println(\"****** Finished DynamicJarLoadingJob accessing class loaded through previous job execution ******\");" +
                "            return String.format(\"See %s/\" + jobConfig.getString(\"logFileName\"), currentDirectory);\n" +
                "        } catch (Exception e) {\n" +
                "            pw.println(\"ERROR: failed with \" + e.getMessage());\n" +
                "            e.printStackTrace(pw);\n" +
                "        } finally {\n" +
                "            pw.flush();\n" +
                "            pw.close();\n" +
                "        }\n" +
                "        return null;\n" +
                "    }" +
                "\n" +
                "    @Override\n" +
                "    public SnappyJobValidation isValidJob(SnappySession spark, Config jobConfig) {\n" +
                "        return new SnappyJobValid();\n" +
                "    }\n" +
                "}";
        return SnappyTestUtils.createCompiledClass(className,
                new File(destDir),
                SnappyTestUtils.getJavaSourceFromString(className, generalClassText),
                new scala.collection.mutable.ArrayBuffer<URL>());
    }

    /**
     * Executes gfxd install-jar command using specified jar file.
     */
    public static synchronized void HydraTask_executeInstallJarCommand() {
        String jarName = SnappyPrms.getUserAppJar();
        String jarIdentifier = SnappyPrms.getJarIdentifier();
        executeCommand(jarName, jarIdentifier, "install-jar");
    }

    /**
     * Executes gfxd install-jar command using dynamically created jar file.
     */
    public static synchronized void HydraTask_executeInstallJarCommand_DynamicJarLoading() {
        String jarName = createJarWithOnlyClasses(3, "1");
        executeCommand(jarName, null, "install-jar");
    }

    /**
     * Executes gfxd modify-jar command using dynamically created jar file.
     */
    public static synchronized void HydraTask_executeReplaceJarCommand_DynamicJarLoading() {
        String jarName = createJarWithOnlyClasses(2, "2");
        executeCommand(jarName, null, "replace-jar");
    }

    /**
     * Executes gfxd modify-jar command using dynamically created jar file.
     */
    public static synchronized void HydraTask_executeRemoveJarCommand_DynamicJarLoading() {
        String jarName = createJarWithOnlyClasses(2, "2");
        executeCommand(jarName, null, "remove-jar");
    }

    /**
     * Executes dynamically created snappy job which uses the classes loaded through gfxd install-jar/replace-jar command.
     */
    public static synchronized void HydraTask_executeSnappyJob_DynamicJarLoading() {
        String jarName = createJarFileWithOnlyJobClass(getTempDir());
        String appName = "myApp_" + System.currentTimeMillis();
        executeSnappyJobWithDynamicJarLoading(jarName, "snappyJobResult_thread_", appName);
    }

    /**
     * Executes dynamically created snappy job which uses the classes from the same jar file used while executing the job.
     */
    public static synchronized void HydraTask_executeSnappyJob_DynamicJarLoading_WithClasses() {
        String appJar = createJarFile(3, "1");
        executeSnappyJobWithDynamicJarLoading(appJar, "snappyJobResultWithClasses_thread_", null);
    }

    /**
     * Executes dynamically created snappy job which accesses the class loaded through previous job execution and the classes from the same jar file used while executing the job.
     */
    public static synchronized void HydraTask_executeSnappyJob_DynamicJarLoading_verifyCleanUp() {
        String appJar = createJarFileWithJobAccessingClassFromPreviousJobExecution(getTempDir(), 2, "2");
        executeSnappyJobWithDynamicJarLoading(appJar, "snappyJobResultAccessingClassFromPreviousJobExecution_thread_", null);
    }

    protected static String createJarFileWithJobAccessingClassFromPreviousJobExecution(String dir, int numClasses, String version) {
        List files = createClasses(numClasses, version, dir);
        files.add(createPersonClass("Person", dir));
        files.add(createJobAccessingClassFromPreviousJobExecution("DynamicJarLoadingJob", dir));
        return SnappyTestUtils.createJarFile((JavaConversions.asScalaBuffer(files)).toList(), dir);
    }

    /**
     * Executes dynamically created snappy job which uses the classes loaded through gfxd install-jar/replace-jar command.
     * The Job class name is identical with the job class generated using HydraTask_executeSnappyJob_DynamicJarLoading method, but the job functionality is different.
     */
    public static synchronized void HydraTask_executeSnappyJobWithIdenticalName() {
        String jarName = createJarFileWithIdenticalJobClass(getTempDir());
        String appName = "myApp_" + System.currentTimeMillis();
        executeSnappyJobWithDynamicJarLoading(jarName, "snappyJobResult_WithIdenticalName_thread_", appName);
    }

    protected static synchronized void executeCommand(String jarName, String jarIdentifier, String command) {
        File log = null, logFile;
        if (jarName == null) {
            String s = "No jarName name provided for executing" + command + " command in Hydra TASK";
            throw new TestException(s);
        }
        if (jarIdentifier == null) {
            jarIdentifier = "APP.myjar";
        }
        try {
            String jarFilePath = snappyTest.getUserAppJarLocation(jarName, jarPath);
            if (jarFilePath == null)
                jarFilePath = snappyTest.getUserAppJarLocation(jarName, getTempDir());
            log = new File(".");
            String dest = log.getCanonicalPath() + File.separator + "installJarResult.log";
            logFile = new File(dest);
            String primaryLocatorHost = (String) SnappyBB.getBB().getSharedMap().get("primaryLocatorHost");
            String primaryLocatorPort = (String) SnappyBB.getBB().getSharedMap().get("primaryLocatorPort");
            ProcessBuilder pb;
            if (command == "remove-jar") {
                pb = new ProcessBuilder(SnappyShellPath, command, "-name=" + jarIdentifier,
                        "-client-port=" + primaryLocatorPort, "-client-bind-address=" + primaryLocatorHost);
            } else {
                pb = new ProcessBuilder(SnappyShellPath, command, "-file=" + jarFilePath, "-name=" + jarIdentifier,
                        "-client-port=" + primaryLocatorPort, "-client-bind-address=" + primaryLocatorHost);
            }
            snappyTest.executeProcess(pb, logFile);
        } catch (IOException e) {
            throw new TestException("IOException occurred while retriving destination logFile path " + log + "\nError Message:" + e.getMessage());
        }
    }
}
