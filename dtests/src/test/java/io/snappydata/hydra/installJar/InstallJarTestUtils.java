/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

import org.apache.spark.SnappyTestUtils;
import org.apache.spark.sql.SnappyContext;

import java.io.PrintWriter;

public class InstallJarTestUtils {

    public static void verify(SnappyContext snc, String version, PrintWriter pw, int numServers, boolean expectedException) {
        try {
            pw.println("Class version : " + version);
            if (version.equalsIgnoreCase("1")) {
                SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass0", "1", numServers, pw);
                SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass1", "1", numServers, pw);
                SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass2", "1", numServers, pw);
                SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass3", "1", numServers, pw);
            } else {
                SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass0", "2", numServers, pw);
                SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass1", "2", numServers, pw);
                SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass2", "2", numServers, pw);
            }
        } catch (Exception e) {
            if (expectedException && e.getMessage().contains("java.lang.ClassNotFoundException")) {
                pw.println("Got expected java.lang.ClassNotFoundException.....");
                pw.flush();
            } else if (!expectedException) {
                //throw new util.TestException("Exception occurred while executing the job " + "\nError Message:" + e.getMessage());
                pw.println("Exception occurred while executing the job " + "\nError Message:" + e.getMessage());
                pw.flush();
            }
        }
    }

    public static void verifyClassFromPreviousJobExecution(SnappyContext snc, String version, PrintWriter pw, int numServers, boolean expectedException) {
        try {
            pw.println("Class version : " + version);
            if (version.equalsIgnoreCase("1")) {
                SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass0", "1", numServers, pw);
                SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass1", "1", numServers, pw);
                SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass2", "1", numServers, pw);
                SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass3", "1", numServers, pw);
            } else {
                SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass0", "2", numServers, pw);
                SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass1", "2", numServers, pw);
                SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass2", "2", numServers, pw);
                SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass3", "1", numServers, pw);
            }
        } catch (Exception e) {
            if (expectedException && e.getMessage().contains("java.lang.ClassNotFoundException")) {
                pw.println("Got expected java.lang.ClassNotFoundException.....");
                pw.flush();
            } else if (!expectedException) {
                //throw new util.TestException("Exception occurred while executing the job " + "\nError Message:" + e.getMessage());
                pw.println("Exception occurred while executing the job " + "\nError Message:" + e.getMessage());
                pw.flush();
            }
        }
    }
}
