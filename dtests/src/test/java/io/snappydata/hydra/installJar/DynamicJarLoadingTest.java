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
        Log.getLogWriter().info("SS - tempDir is : " + tempDir.getAbsolutePath());
        return tempDir.getAbsolutePath();
    }

    protected static String createJarFile(int numClasses, String classVersion) {
        String dir = getTempDir();
        List files = new ArrayList();
        for (int i = 0; i <= numClasses; i++) {
            files.add(createSupportiveClasses("FakeClass" + i, classVersion, dir));
        }
        files.add(createJobClass("DynamicJarLoadingJob", dir));
        return SnappyTestUtils.createJarFile((JavaConversions.asScalaBuffer(files)).toList(), dir);
    }

    public static void HydraTask_executeSnappyJobWithDynamicJarLoading_installJar() {
        Log.getLogWriter().info("SS - entered into HydraTask_executeSnappyJobWithDynamicJarLoading_installJar....");
        String appJar = createJarFile(3, "1");
        executeSnappyJobWithDynamicJarLoading(appJar, "snappyJobInstallJarResult_thread_");
        Log.getLogWriter().info("SS - Done with HydraTask_executeSnappyJobWithDynamicJarLoading_installJar....");

    }

    public static void HydraTask_executeSnappyJobWithDynamicJarLoading_modifyJar() {
        Log.getLogWriter().info("SS - entered into HydraTask_executeSnappyJobWithDynamicJarLoading_modifyJar....");
        String appJar = createJarFile(2, "2");
        executeSnappyJobWithDynamicJarLoading(appJar, "snappyJobModifyJarResult_thread_");
        Log.getLogWriter().info("SS - Done with HydraTask_executeSnappyJobWithDynamicJarLoading_modifyJar....");

    }

    protected static void executeSnappyJobWithDynamicJarLoading(String appJar, String logFileName){
        Log.getLogWriter().info("SS - entered into HydraTask_executeSnappyJobWithDynamicJarLoading....");
        int currentThread = snappyTest.getMyTid();
        String logFile = logFileName + currentThread + "_" + System.currentTimeMillis() + ".log";
        Log.getLogWriter().info("SS - appJar location in HydraTask_executeSnappyJobWithDynamicJarLoading is : " + appJar);
        snappyTest.executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile, appJar, getTempDir());
        Log.getLogWriter().info("SS - Done with HydraTask_executeSnappyJobWithDynamicJarLoading....");
    }

    public static File createSupportiveClasses(String className, String version, String destDir) {
        File dir = new File(destDir);
        String generalClasseText = "public class " + className + " implements java.io.Serializable {" +
                "  @Override public String toString() { return \"" + version + "\"; }}";
        Log.getLogWriter().info("SS - destDir in createSupportiveClasses : " + dir);
        Log.getLogWriter().info("SS - generalClasseText String in createSupportiveClasses is : " + generalClasseText);
        Log.getLogWriter().info("SS - SnappyTestUtils.getJavaSourceFromString in createSupportiveClasses: " + SnappyTestUtils.getJavaSourceFromString(className, generalClasseText));
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
                "        try (PrintWriter pw = new PrintWriter(new FileOutputStream(new File(jobConfig.getString(\"logFileName\"))), true)){\n" +
                "            String currentDirectory = new File(\".\").getCanonicalPath();\n" +
                "            io.snappydata.hydra.installJar. TestUtils.verify(snc, jobConfig.getString(\"classVersion\"), pw);\n" +
                "            return String.format(\"See %s/\" + jobConfig.getString(\"logFileName\"), currentDirectory);\n" +
                "        } catch (Exception e) {\n" +
                "            StringWriter sw = new StringWriter();\n" +
                "            PrintWriter spw = new PrintWriter(sw);\n" +
                "            spw.println(\"ERROR: failed with \" + e);\n" +
                "            e.printStackTrace(spw);\n" +
                "            return spw.toString();\n" +
                "        }\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    public SnappyJobValidation isValidJob(SnappyContext snc, Config config) {\n" +
                "        return new SnappyJobValid();\n" +
                "    }\n" +
                "}";
        Log.getLogWriter().info("SS - destDir in createJobClass : " + destDir);
        Log.getLogWriter().info("SS - generalClasseText String is : " + generalClassText);
        System.out.println("swati - generalClasseText String is : " + generalClassText.split("\n")[2]);
        Log.getLogWriter().info("SS - SnappyTestUtils.getJavaSourceFromString : " + SnappyTestUtils.getJavaSourceFromString(className, generalClassText));
        return SnappyTestUtils.createCompiledClass(className,
                new File(destDir),
                SnappyTestUtils.getJavaSourceFromString(className, generalClassText),
                new scala.collection.mutable.ArrayBuffer<URL>());
    }

}
