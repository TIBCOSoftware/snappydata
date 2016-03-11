package io.snappydata.hydra.cluster;


import com.gemstone.gemfire.LogWriter;
import hydra.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SnappyContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.junit.Assert;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;

public class SnappyTest implements Serializable {

    private static transient JavaSparkContext jsc;
    private static transient SnappyContext snc;
    protected static SnappyTest snappyTest;
    private static char sep;
    private static HostDescription hd;
    private static String columnTable = "AIRLINE";
    private static String rowTable = "AIRLINEREF";
    private static String stagingColumnTable = "STAGING_AIRLINE";
    private static String stagingRowTable = "STAGING_AIRLINEREF";

//
//    protected scala.collection.immutable.Map<String, String> props = scala.collection.immutable.Map$.<String, String> empty();

    public static <A, B> Map<A, B> toScalaMap(HashMap<A, B> m) {
        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(Predef.<Tuple2<A, B>>conforms());
    }

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

    public static void HydraTask_createRowColumnTableUsingSQL() {
        HydraTask_initializeSnappyTest();
        String hfile = snappyTest.getDataLocation() + "/airlineParquetData";
        String reffile = snappyTest.getDataLocation() + "/airportcodeParquetData";

        snc.sql("CREATE TABLE " + stagingColumnTable + " \n" +
                " USING parquet OPTIONS(path '" + hfile + "')");
        Log.getLogWriter().info("SS - staging column table " + stagingColumnTable + " created successfully");
        snc.sql("CREATE TABLE " + columnTable + " USING column OPTIONS(buckets '11') AS (\n" +
                "  SELECT Year AS yeari, Month AS monthi , DayOfMonth,\n" +
                "    DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime,\n" +
                "    UniqueCarrier, FlightNum, TailNum, ActualElapsedTime,\n" +
                "    CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin,\n" +
                "    Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode,\n" +
                "    Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay,\n" +
                "    LateAircraftDelay, ArrDelaySlot\n" +
                "    FROM " + stagingColumnTable + ")");
        Log.getLogWriter().info("SS - column table " + columnTable + " created successfully");
        snc.sql("CREATE TABLE " + stagingRowTable + " \n" +
                "  USING parquet OPTIONS(path '" + reffile + "')");
        Log.getLogWriter().info("SS - staging row table " + stagingRowTable + " created successfully");
        snc.sql("CREATE TABLE " + rowTable + " USING row OPTIONS() AS (SELECT CODE, DESCRIPTION FROM " + stagingRowTable + ")");
        Log.getLogWriter().info("SS - row table " + rowTable + " created successfully");
    }


    public static void HydraTask_createRowColumnTableUsingDataFrame() {
        HydraTask_initializeSnappyTest();
        String hfile = snappyTest.getDataFrameDataLocation() + "/2015.parquet";
        String reffile = snappyTest.getDataLocation() + "/airportcodeParquetData";
        DataFrame airlineDataFrame, airlineRefDataFrame;
        HashMap<String, String> props = new HashMap<String, String>();
//        props.put("buckets","11");
        airlineDataFrame = snc.read().load(hfile);
        snc.read().load(hfile).registerTempTable(stagingColumnTable);
        Log.getLogWriter().info("SS - airline dataframe loaded successfully");
        snc.dropTable(columnTable, true);
        snc.createTable(columnTable, "column", airlineDataFrame.schema(), toScalaMap(props));
        airlineDataFrame.write().format("column").mode(SaveMode.Append).saveAsTable(columnTable);
        Log.getLogWriter().info("SS - column table " + columnTable + " created successfully");
        airlineRefDataFrame = snc.read().load(reffile);
        snc.read().load(reffile).registerTempTable(stagingRowTable);
        props.clear();
        Log.getLogWriter().info("SS - airlineref dataframe loaded successfully");
        snc.dropTable(rowTable, true);
        snc.createTable(rowTable, "row", airlineRefDataFrame.schema(), toScalaMap(props));
        airlineRefDataFrame.write().format("row").mode(SaveMode.Append).saveAsTable(rowTable);
        Log.getLogWriter().info("SS - row table " + rowTable + " created successfully");
    }

    public static void HydraTask_dropRowColumnTable() {
        snc.dropTable(stagingColumnTable, true);
        snc.dropTable(columnTable, true);
        snc.dropTable(stagingRowTable, true);
        snc.dropTable(rowTable, true);
    }

    /**
     * This method gives simple count(*) query results on airline table.
     */

    public static void HydraTask_countQueryOnColumnTable() {
        String query1 = "SELECT count(*) FROM airline";
        Row[] result = snc.sql(query1).collect();
        Assert.assertEquals(1, result.length);
        Long expectedValue = Long.valueOf(1000000);
        Assert.assertEquals(expectedValue, ((GenericRow) result[0]).get(0));
        Log.getLogWriter().info("SS - Qyery executed successfully and query result is ::" + ((GenericRow) result[0]).get(0));
    }


    /**
     * This method retrives which airline had the most flights each year.
     */

    public static void HydraTask_countQueryWithGroupByOrderBy() {
        String query = "select  count(*) flightRecCount, description AirlineName, UniqueCarrier carrierCode ,yeari \n" +
                "   from airline , airlineref\n" +
                "   where airline.UniqueCarrier = airlineref.code\n" +
                "   group by UniqueCarrier,description, yeari \n" +
                "   order by flightRecCount desc limit 10 ";
        Row[] result = snc.sql(query).collect();
        Long expectedValue = Long.valueOf(190214);
        Assert.assertEquals(expectedValue, ((GenericRow) result[0]).get(0));
        Log.getLogWriter().info("SS - Qyery executed successfully and query result is ::" + ((GenericRow) result[0]).get(0));
    }

    /**
     * This method retrives which Airlines Arrive On Schedule
     */

    public static void HydraTask_avgArrDelayWithGroupByOrderByForSchedule() {
        String query = "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier from airline   \n" +
                "   group by UniqueCarrier\n" +
                "   order by arrivalDelay ";
        Row[] result = snc.sql(query).collect();
        Log.getLogWriter().info("SS - Qyery executed successfully and query result is ::" + ((GenericRow) result[0]).get(0));
    }

    /**
     * This method retrives which Airlines Arrive On Schedule. JOIN with reference table.
     */

    public static void HydraTask_avgArrDelayWithGroupByOrderByWithJoinForSchedule() {
        String query = "select AVG(ArrDelay) arrivalDelay, description AirlineName, UniqueCarrier carrier \n" +
                "  from airline, airlineref\n" +
                "  where airline.UniqueCarrier = airlineref.Code \n" +
                "  group by UniqueCarrier, description \n" +
                "  order by arrivalDelay ";
        Row[] result = snc.sql(query).collect();
        Log.getLogWriter().info("SS - Qyery executed successfully and query result is ::" + ((GenericRow) result[0]).get(0));
    }

    /**
     * This method retrives the trend in arrival delays across all airlines in the US
     */

    public static void HydraTask_avgArrDelayWithGroupByOrderByForTrendAnalysis() {
        String query = "select AVG(ArrDelay) ArrivalDelay, yeari\n" +
                "  from airline \n" +
                "  group by yeari \n" +
                "  order by yeari ";
        Row[] result = snc.sql(query).collect();
//        Long expectedValue = Long.parseLong("6.735443");
//        Assert.assertEquals(expectedValue, ((GenericRow) result[0]).get(0));
        Log.getLogWriter().info("SS - Qyery executed successfully and query result is ::" + ((GenericRow) result[0]).get(0));
    }

    /**
     * This method retrives Which airline out of SanFrancisco had most delays due to weather
     */

    public static void HydraTask_sumWeatherDelayWithGroupByWithLimit() {
        String query = "SELECT sum(WeatherDelay) totalWeatherDelay, airlineref.DESCRIPTION \n" +
                "  FROM airline, airlineref \n" +
                "  WHERE airline.UniqueCarrier = airlineref.CODE AND  Origin like '%SFO%' AND WeatherDelay > 0 \n" +
                "  GROUP BY DESCRIPTION \n" +
                "  limit 20 ";
        Row[] result = snc.sql(query).collect();
        Log.getLogWriter().info("SS - Qyery executed successfully and query result is ::" + ((GenericRow) result[0]).get(0));
    }

    /**
     * This method updates the description for a code in row table. Suppose a particular Airline company say 'Delta Air Lines Inc.' re-brands itself as 'Delta America'
     * --- the airline code can be updated in the row table
     */

    public static void HydraTask_updateRowTable() {
        String query1 = "UPDATE AIRLINEREF SET DESCRIPTION='Delta America' WHERE CODE='DL' ";
        Row[] result = snc.sql(query1).collect();
        Log.getLogWriter().info("SS - Qyery executed successfully");
        String query2 = "SELECT * FROM AIRLINEREF WHERE CAST(CODE AS VARCHAR(25))='DL'";
        Row[] result1 = snc.sql(query2).collect();
//        Long expectedValue = Long.parseLong("DL");
//        Assert.assertEquals(expectedValue, ((GenericRow) result[0]).get(0));
        Log.getLogWriter().info("SS - Qyery executed successfully and query result is ::" + ((GenericRow) result1[0]).get(0));
    }

    protected String getDataLocation() {
        String scriptPath = null;
        try {
            hd = getHostDescription();
            scriptPath = hd.getGemFireHome() + sep + ".." + sep + "snappy"
                    + sep + "quickstart" + sep + "data";
            Log.getLogWriter().info("SS - scriptPath is ::" + scriptPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return scriptPath;
    }

    protected String getDataFrameDataLocation() {
        String scriptPath = null;
        try {
            hd = getHostDescription();
            scriptPath = hd.getGemFireHome() + sep + ".." + sep + ".."
                    + sep + ".." + sep + "tests-common" + sep + "src" + sep + "main" + sep + "resources";
            Log.getLogWriter().info("SS - scriptPath is ::" + scriptPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return scriptPath;
    }

    protected HostDescription getHostDescription() {
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
