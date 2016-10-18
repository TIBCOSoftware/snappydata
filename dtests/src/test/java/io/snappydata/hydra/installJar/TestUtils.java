package io.snappydata.hydra.installJar;

import org.apache.spark.sql.SnappyContext;

import java.io.PrintWriter;

/**
 * Created by swati on 26/8/16.
 */
public class TestUtils {


    public static void verify(SnappyContext snc, String version, PrintWriter pw) throws Exception {
        /*int numServers = (int) SnappyBB.getBB().getSharedCounters().read(SnappyBB.numServers);
        pw.println("SS - SnappyBB.numServers : " + numServers);*/
        pw.println("SS - version : " + version);
        //pw.println(new TestException("Here---------------"));
        /*if (version.equalsIgnoreCase("1")) {
            SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass1", "1", numServers);
            SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass2", "1", numServers);
            SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass3", "1", numServers);
            SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass4", "1", numServers);
        } else {
            SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass1", "2", numServers);
            SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass2", "2", numServers);
            SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass5", "2", numServers);
        }*/
    }
}
