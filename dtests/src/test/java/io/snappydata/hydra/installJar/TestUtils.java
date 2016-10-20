package io.snappydata.hydra.installJar;

import org.apache.spark.SnappyTestUtils;
import org.apache.spark.sql.SnappyContext;

import java.io.PrintWriter;

/**
 * Created by swati on 26/8/16.
 */
public class TestUtils {

    public static void verify(SnappyContext snc, String version, PrintWriter pw, int numServers) throws Exception {
        pw.println("SS - version : " + version);
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
    }
}
