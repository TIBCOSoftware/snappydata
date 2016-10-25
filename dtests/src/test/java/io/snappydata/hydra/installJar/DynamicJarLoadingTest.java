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
import hydra.Log;
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
        List files = new ArrayList();
        for (int i = 0; i <= numClasses; i++) {
            files.add(createSupportiveClasses("FakeClass" + i, classVersion, dir));
        }
//        files = createJarWithOnlyClasses(numClasses, classVersion);
        files.add(createJobClass("DynamicJarLoadingJob", dir));
        return SnappyTestUtils.createJarFile((JavaConversions.asScalaBuffer(files)).toList(), dir);
    }

    protected static String createJarWithOnlyClasses(int numClasses, String classVersion) {
        String dir = getTempDir();
        List files = new ArrayList();
        for (int i = 0; i <= numClasses; i++) {
            files.add(createSupportiveClasses("FakeClass" + i, classVersion, dir));
        }
        return SnappyTestUtils.createJarFile((JavaConversions.asScalaBuffer(files)).toList(), dir);
    }

    protected static String createJarFileWithOnlyJobClass() {
        String dir = getTempDir();
        List files = new ArrayList();
        files.add(createJobClass("DynamicJarLoadingJob", dir));
        return SnappyTestUtils.createJarFile((JavaConversions.asScalaBuffer(files)).toList(), dir);
    }

 /*   protected static String createJarFileWithOnlyClasses(int numClasses, String classVersion) {
        String dir = getTempDir();
        List files = new ArrayList();
        for (int i = 0; i <= numClasses; i++) {
            files.add(createSupportiveClasses("FakeClass" + i, classVersion, dir));
        }
       return SnappyTestUtils.createJarFile((JavaConversions.asScalaBuffer(files)).toList(), dir);
    }*/

    public static void HydraTask_executeSnappyJobWithDynamicJarLoading_installJar() {
        String appJar = createJarFile(3, "1");
        executeSnappyJobWithDynamicJarLoading(appJar, "snappyJobInstallJarResult_thread_");
    }

    public static void HydraTask_executeSnappyJobWithDynamicJarLoading_modifyJar() {
        String appJar = createJarFile(2, "2");
        executeSnappyJobWithDynamicJarLoading(appJar, "snappyJobModifyJarResult_thread_");
    }

    protected static void executeSnappyJobWithDynamicJarLoading(String appJar, String logFileName) {
        int currentThread = snappyTest.getMyTid();
        String logFile = logFileName + currentThread + "_" + System.currentTimeMillis() + ".log";
        snappyTest.executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile, appJar, getTempDir());
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
                "import org.apache.spark.sql.SnappyContext;\n" +
                "import org.apache.spark.sql.SnappyJobValid;\n" +
                "import org.apache.spark.sql.SnappyJobValidation;\n" +
                "import org.apache.spark.sql.SnappySQLJob;\n" +
                "\n" +
                "import java.io.File;\n" +
                "import java.io.FileOutputStream;\n" +
                "import java.io.PrintWriter;\n" +
                "import java.io.StringWriter;\n" +
                "\n" +
                "public class DynamicJarLoadingJob extends SnappySQLJob {\n" +
                "    @Override\n" +
                "    public Object runSnappyJob(SnappyContext snc, Config jobConfig) {\n" +
                "        PrintWriter pw = null;\n" +
                "        try {\n" +
                "            pw = new PrintWriter(new FileOutputStream(new File(jobConfig.getString(\"logFileName\")), true));\n" +
                "            int numServers = Integer.parseInt(jobConfig.getString(\"numServers\"));\n" +
                "            pw.println(\"****** DynamicJarLoadingJob started ******\");\n" +
                "            String currentDirectory = new File(\".\").getCanonicalPath();\n" +
                "            io.snappydata.hydra.installJar.TestUtils.verify(snc, jobConfig.getString(\"classVersion\"), pw, numServers);\n" +
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
                "    public SnappyJobValidation isValidJob(SnappyContext snc, Config config) {\n" +
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
     * Executes dynamically created snappy job which uses the classes loaded through gfxd install-jar/replace-jar command.
     */
    public static synchronized void HydraTask_executeSnappyJob_DynamicJarLoading() {
        String jarName = createJarFileWithOnlyJobClass();
        executeSnappyJobWithDynamicJarLoading(jarName, "snappyJob_InstallJarResult_thread_");
    }

    protected static synchronized void executeCommand(String jarName, String jarIdentifier, String command) {
        File log = null, logFile = null;
        if (jarName == null) {
            String s = "No jarName name provided for executing install-jar command in Hydra TASK";
            throw new TestException(s);
        }
        if (jarIdentifier == null) {
            jarIdentifier = "APP.myjar";
        }
        try {
            String jarFilePath = snappyTest.getUserAppJarLocation(jarName, jarPath);
            if (jarFilePath == null)
                jarFilePath = snappyTest.getUserAppJarLocation(jarName, getTempDir());
            Log.getLogWriter().info("SS - jarFilePath is : " + jarFilePath);
            Log.getLogWriter().info("SS - jarIdentifier is : " + jarIdentifier);
            log = new File(".");
            String dest = log.getCanonicalPath() + File.separator + "installJarResult.log";
            logFile = new File(dest);
            String primaryLocatorHost = (String) SnappyBB.getBB().getSharedMap().get("primaryLocatorHost");
            String primaryLocatorPort = (String) SnappyBB.getBB().getSharedMap().get("primaryLocatorPort");
            ProcessBuilder pb = new ProcessBuilder(SnappyShellPath, command, "-file=" + jarFilePath, "-name=" + jarIdentifier,
                    "-client-port=" + primaryLocatorPort, "-client-bind-address=" + primaryLocatorHost);
            Log.getLogWriter().info("SS - pb command is : " + pb.command() + ":" + pb.toString());
            snappyTest.executeProcess(pb, logFile);
        } catch (IOException e) {
            throw new TestException("IOException occurred while retriving destination logFile path " + log + "\nError Message:" + e.getMessage());
        }
    }
}
