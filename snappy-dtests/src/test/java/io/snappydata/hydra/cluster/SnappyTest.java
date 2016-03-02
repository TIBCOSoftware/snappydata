package io.snappydata.hydra.cluster;


import com.gemstone.gemfire.LogWriter;
import hydra.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SnappyContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.junit.Assert;

import java.io.IOException;
import java.io.Serializable;


public class SnappyTest implements Serializable {

    private static transient JavaSparkContext jsc;
    private static transient SnappyContext snc;
    protected static SnappyTest snappyTest;
    private static char sep;
    private static HostDescription hd;

    public static void HydraTask_initializeSnappy() {
        org.apache.spark.SparkConf conf = new org.apache.spark.SparkConf().setAppName("SnappyTest").setMaster("local").set("spark.ui.port", "6060");
        jsc = new JavaSparkContext(conf);
        snc = SnappyContext.getOrCreate(JavaSparkContext.toSparkContext(jsc));
        Log.getLogWriter().info("SS - SnappyContext initialized successfully");
    }


    public static void HydraTask_stopSnappy() {
        snc.stop();
        Log.getLogWriter().info("SS - SnappyContext stopped successfully");

    }

    public static void createDataFrameAndRowColumnTable() {
        HydraTask_initializeSnappyTest();
        String hfile = snappyTest.getDataLocation() + "/airlineParquetData";
        String reffile = snappyTest.getDataLocation() + "/airportcodeParquetData";
        DataFrame airlineDataFrame, airlineRefDataFrame;
        airlineDataFrame = snc.read().load(hfile);
        Log.getLogWriter().info("SS - airline dataframe loaded successfully");
        airlineRefDataFrame = snc.read().load(reffile);
        Log.getLogWriter().info("SS - airlineref dataframe loaded successfully");
        airlineDataFrame.registerTempTable("airline");
        airlineRefDataFrame.registerTempTable("airlineref");
        snc.cacheTable("airline");
        Log.getLogWriter().info("SS - airline table cached successfully");
        snc.cacheTable("airlineref");
        Log.getLogWriter().info("SS - airlineref table cached successfully");
    }

    public static void queryColumnTable() {
        String query1 = "SELECT count(*) FROM airline";
        Row[] result = snc.sql(query1).collect();
        Assert.assertEquals(1, result.length);
        Long expectedValue = Long.valueOf(1000000);
        Assert.assertEquals(expectedValue, ((GenericRow) result[0]).get(0));
        Log.getLogWriter().info("SS - Qyery executed successfully and query result is ::" + ((GenericRow) result[0]).get(0));
    }

    protected String getDataLocation() {
        String scriptPath = null;
        try {
            Log.getLogWriter().info("SS - Entering into getDataLocation method....");
            hd = getHostDescription();
            scriptPath = hd.getGemFireHome() + sep + ".." + sep + "snappy"
                    + sep + "quickstart" + sep + "data";
            Log.getLogWriter().info("SS - scriptPath is ::" + scriptPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return scriptPath;
    }

    protected HostDescription getHostDescription() {
        Log.getLogWriter().info("SS - Entering into getHostDescription method....");
        hd = TestConfig.getInstance()
                .getClientDescription(RemoteTestModule.getMyClientName())
                .getVmDescription().getHostDescription();
        sep = hd.getFileSep();
        Log.getLogWriter().info("SS - gemfireHome is ::" + hd.getGemFireHome());
        return hd;
    }

    protected String getScriptLocation(String scriptName) {
        String scriptPath = null;
        try {
            Log.getLogWriter().info("SS - Entering into getScriptLocation method....");
            hd = getHostDescription();
            scriptPath = hd.getGemFireHome() + sep + ".." + sep + "snappy"
                    + sep + "sbin" + sep + scriptName;
            Log.getLogWriter().info("SS - scriptPath is ::" + scriptPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return scriptPath;
    }

    public static void HydraTask_initializeSnappyTest() {
        if (snappyTest == null) {
            snappyTest = new SnappyTest();
        }
    }

    /**
     * Create and start snappy locator.
     */
    public static void HydraTask_createAndStartSnappyLocator() {
        HydraTask_initializeSnappyTest();
        try {
            Process pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-locators.sh"), "start").start();
            int rc = pb.waitFor();
            System.out.printf("Script executed with exit code %d\n", rc);
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }

    /**
     * Create and start snappy server.
     */
    public static void HydraTask_createAndStartSnappyServers() {
        HydraTask_initializeSnappyTest();
        try {
            Process pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-servers.sh"), "start").start();
            int rc = pb.waitFor();
            System.out.printf("Script executed with exit code %d\n", rc);
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }

    /**
     * Creates and start snappy lead.
     */
    public static void HydraTask_createAndStartSnappyLeader() {
        HydraTask_initializeSnappyTest();
        try {
            Process pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-leads.sh"), "start").start();
            int rc = pb.waitFor();
            System.out.printf("Script executed with exit code %d\n", rc);
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }

    /**
     * Stops snappy lead.
     */
    public static void HydraTask_stopSnappyLeader() {
        try {
            Process pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-leads.sh"), "stop").start();
            int rc = pb.waitFor();
            System.out.printf("Script executed with exit code %d\n", rc);
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }

    /**
     * Stops snappy server/servers.
     */
    public static void HydraTask_stopSnappyServers() {
        try {
            Process pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-servers.sh"), "stop").start();
            int rc = pb.waitFor();
            System.out.printf("Script executed with exit code %d\n", rc);
        } catch (IOException e1) {
            e1.printStackTrace();

        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }

    /**
     * Stops a snappy locator.
     */
    public static void HydraTask_stopSnappyLocator() {
        try {
            Process pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-locators.sh"), "stop").start();
            int rc = pb.waitFor();
            System.out.printf("Script executed with exit code %d\n", rc);
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }

    public static void HydraTask_stopSnappyCluster() {
        try {
            Process pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-stop-all.sh")).start();
            int rc = pb.waitFor();
            System.out.printf("Script executed with exit code %d\n", rc);
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }

    protected LogWriter log() {
        return Log.getLogWriter();
    }

}
