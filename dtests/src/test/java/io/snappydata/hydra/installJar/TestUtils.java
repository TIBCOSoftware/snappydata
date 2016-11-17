package io.snappydata.hydra.installJar;

import org.apache.spark.SnappyTestUtils;
import org.apache.spark.sql.SnappyContext;
import util.TestException;

import java.io.PrintWriter;

/**
 * Created by swati on 26/8/16.
 */
public class TestUtils {

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
            } else if (!expectedException) {
                throw new TestException("Exception occurred while executing the job " + "\nError Message:" + e.getMessage());
            }
        }
    }
}
