package io.snappydata.hydra.udfs;

import hydra.Log;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.test.util.TestException;

import java.sql.*;

public class UDFInterface extends SnappyTest {

    /**
     * This method establish the connection with Snappy SQL via JDBC.
     *
     * @throws SQLException if did not get connection with Snappy Locator.
     */
    private Connection getJDBCConnection() {
        Connection connection;
        try {
            connection = getLocatorConnection();
        } catch (SQLException se) {
            throw new TestException("Got exception while establish the connection.....", se);
        }
        return connection;
    }

    private void createTables(Connection jdbcConnection) {
        try {
            Log.getLogWriter().info("Creating the tables...");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T1;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T2;");

            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T1(id Integer) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T1 VALUES(25);");

            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T2(s1 String, s2 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T2 VALUES('Snappy','Data')");


        } catch(SQLException se) {
            throw new TestException(se.getMessage() + ", Error in creating and ingesting the data.");
        }
    }

    private void dropTables(Connection jdbcConnection) {
        try {
            Log.getLogWriter().info("Dropping the tables...");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T1;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T2;");
        } catch(SQLException se) {
            throw new TestException(se.getMessage() + ", Error in droping the tables.");
        }
    }

    private void createJavaUDF(Connection jdbcConnection) {
        try {
            Log.getLogWriter().info("Creating the Java UDFs...");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF1 AS com.snappy.poc.udf.JavaUDF1 RETURNS INTEGER USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF2 AS com.snappy.poc.udf.JavaUDF2 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF3 AS com.snappy.poc.udf.JavaUDF3 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF4 AS com.snappy.poc.udf.JavaUDF4 RETURNS DOUBLE USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF5 AS com.snappy.poc.udf.JavaUDF5 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF6 AS com.snappy.poc.udf.JavaUDF6 RETURNS DECIMAL USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF7 AS com.snappy.poc.udf.JavaUDF7 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF8 AS com.snappy.poc.udf.JavaUDF8 RETURNS SHORT USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF9 AS com.snappy.poc.udf.JavaUDF9 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF10 AS com.snappy.poc.udf.JavaUDF10 RETURNS FLOAT USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF11 AS com.snappy.poc.udf.JavaUDF11 RETURNS BOOLEAN USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF12 AS com.snappy.poc.udf.JavaUDF12 RETURNS DATE USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF13 AS com.snappy.poc.udf.JavaUDF13 RETURNS TIMESTAMP USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF14 AS com.snappy.poc.udf.JavaUDF14 RETURNS LONG USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF15 AS com.snappy.poc.udf.JavaUDF15 RETURNS DOUBLE USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF16 AS com.snappy.poc.udf.JavaUDF16 RETURNS VARCHAR(255) USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF17 AS com.snappy.poc.udf.JavaUDF17 RETURNS BOOLEAN USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF18 AS com.snappy.poc.udf.JavaUDF18 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF19 AS com.snappy.poc.udf.JavaUDF19 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF20 AS com.snappy.poc.udf.JavaUDF20 RETURNS INTEGER USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF21 AS com.snappy.poc.udf.JavaUDF21 RETURNS INTEGER USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF22 AS com.snappy.poc.udf.JavaUDF22 RETURNS INTEGER USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + ", Error in creating Java UDF functions.");
        }
    }

    private void executeJavaUDF(Connection jdbcConnecton) {
            getUDFResult(jdbcConnecton,"SELECT UDF1(id) FROM T1;","UDF1");
            getUDFResult(jdbcConnecton,"SELECT UDF2(s1,s2) FROM T2;","UDF2");
//            getUDFResult(jdbcConnecton,"SELECT UDF3(Array('snappydata','udf1testing','successful'),Array('snappydata','udf2testing','successful'),Array('snappydata','udf3testing','successful'));","UDF3");
//            getUDFResult(jdbcConnecton,"SELECT UDF4(Map('Maths',100.0d),Map('Science',96.5d),Map('English',92.0d),Map('Music',98.5d));","UDF4");
//            getUDFResult(jdbcConnecton,"SELECT UDF5(true,true,true,true,true);","UDF5");
//            getUDFResult(jdbcConnecton,"SELECT UDF6(231.45,563.93,899.25,611.98,444.44,999.99);","UDF6");
//            getUDFResult(jdbcConnecton,"SELECT UDF7(10,20,30,40,50,60,Struct(17933,54.68d,'SnappyData',null));","UDF7");
//            getUDFResult(jdbcConnecton,"SELECT UDF8(10L,20L,30L,40L,50L,60L,70L,80L);","UDF8");
//            getUDFResult(jdbcConnecton,"SELECT UDF9('c','h','a','n','d','r','e','s','h');","UDF9");
//            getUDFResult(jdbcConnecton,"SELECT UDF10(12.7f,12.7f,12.7f,12.7f,12.7f,12.7f,12.7f,12.7f,12.7f,12.7f);","UDF10");
//            getUDFResult(jdbcConnecton,"SELECT UDF11(12.1f,12.2f,12.3f,12.4f,12.5f,12.6f,12.7f,12.8f,12.9f,14.1f,12.8f);","UDF11");
//            getUDFResult(jdbcConnecton,"SELECT UDF11(12.1f,12.2f,12.3f,12.4f,12.5f,12.6f,12.7f,12.8f,12.9f,14.1f,15.5f);","UDF11");
//            getUDFResult(jdbcConnecton,"SELECT UDF12('Snappy','Data','Leader','Locator','Server','Cluster','Performance','UI','SQL','DataFrame','Dataset','API');","UDF12");
//            getUDFResult(jdbcConnecton,"SELECT UDF13(10,21,25,54,89,78,63,78,55,13,14,85,71);","UDF13");
//            getUDFResult(jdbcConnecton,"SELECT UDF14('0XFF','10','067','54534','0X897','999','077','1234567891234567','0x8978','015','1005','13177','14736','07777');","UDF14");
//            getUDFResult(jdbcConnecton,"SELECT UDF15(5.23d,99.3d,115.6d,0.9d,78.5d,999.99d,458.32d,133.58d,789.87d,269.21d,333.96d,653.21d,550.32d,489.14d,129.87d);","UDF15");
//            getUDFResult(jdbcConnecton,"SELECT UDF16('snappy','data','is','working','on','a','distributed','data','base.','Its','Performance','is','faster','than','Apache','spark');","UDF16");
//            getUDFResult(jdbcConnecton,"SELECT UDF17(15.2f,17.2f,99.9f,45.2f,71.0f,89.8f,66.6f,23.7f,33.1f,13.5f,77.7f,38.3f,44.4f,21.7f,41.8f,83.2f,78.1f);","UDF17");
//            getUDFResult(jdbcConnecton,"SELECT UDF18('snappy','data','spark','leader','locator','server','memory','eviction','sql','udf','udaf','scala','docker','catalog','smart connector','cores','executor','dir');","UDF18");
//            getUDFResult(jdbcConnecton,"SELECT UDF19('snappy','data','spark','leader','locator','server','memory','eviction','sql','udf','udaf','scala','docker','catalog','smart connector','cores','executor','dir','avro');","UDF19");
//            getUDFResult(jdbcConnecton,"SELECT UDF20(10,20,30,40,50,60,70,80,90,100,13,17,21,25,19,22,86,88,64,58);","UDF20");
//            getUDFResult(jdbcConnecton,"SELECT UDF21('snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy');","UDF21");
//            getUDFResult(jdbcConnecton,"SELECT UDF22(10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10);","UDF22");
    }

    private void getUDFResult(Connection jdbcConnection, String sqlStatement, String udfIndex) {
        ResultSet rs = null;
        Statement st = null;
        String udfType = "Java";

        try {
            if(sqlStatement.contains("Scala" ))
                udfType = "Scala";
            Log.getLogWriter().info("Executing the : " + udfType + " " + udfIndex);
            st = jdbcConnection.createStatement();
            if(st.execute(sqlStatement)) {
                rs = st.getResultSet();
                ResultSetMetaData rsm = rs.getMetaData();
                //Log.getLogWriter().info("Column Count : " + rsm.getColumnCount());
                Log.getLogWriter().info("Column Name : "  + rsm.getColumnName(1));
                String columnName = rsm.getColumnName(1);
                while(rs.next()) {
                    String result = rs.getString(columnName);
                    Log.getLogWriter().info(udfType + "  " + udfIndex + " : " + result);
                }
            }
            st.clearBatch();
            if(udfIndex.equals("UDF22")) {
                if(rs != null)
                    rs.close();
                if(st != null)
                    st.close();
            }
        } catch (SQLException se) {
            throw new TestException(se.getMessage() + ", Error in getting UDF result");
        }
    }

    private void dropJavaUDF(Connection jdbcConnection) {
        try {
            Log.getLogWriter().info("Dropping the Java UDFs...");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF1;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF2;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF3;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF4;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF5;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF6;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF7;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF8;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF9;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF10;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF11;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF12;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF13;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF14;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF15;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF16;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF17;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF18;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF19;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF20;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF21;");
//            jdbcConnection.createStatement().execute("DROP FUNCTION UDF22;");
        } catch(SQLException se) {
            throw new TestException(se.getMessage() + ", Error in dropping Java UDF functions.");
        }
    }

    private void createScalaUDF(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF1 AS com.snappy.scala.poc.udf.ScalaUDF1 RETURNS TIMESTAMP USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF2 AS com.snappy.scala.poc.udf.ScalaUDF2 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF3 AS com.snappy.scala.poc.udf.ScalaUDF3 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF4 AS com.snappy.scala.poc.udf.ScalaUDF4 RETURNS DOUBLE USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF5 AS com.snappy.scala.poc.udf.ScalaUDF5 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF6 AS com.snappy.scala.poc.udf.ScalaUDF6 RETURNS DOUBLE USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF7 AS com.snappy.scala.poc.udf.ScalaUDF7 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF8 AS com.snappy.scala.poc.udf.ScalaUDF8 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF9 AS com.snappy.scala.poc.udf.ScalaUDF9 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF10 AS com.snappy.scala.poc.udf.ScalaUDF10 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF11 AS com.snappy.scala.poc.udf.ScalaUDF11 RETURNS FLOAT USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF12 AS com.snappy.scala.poc.udf.ScalaUDF12 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF13 AS com.snappy.scala.poc.udf.ScalaUDF13 RETURNS DECIMAL USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF13_1 AS com.snappy.scala.poc.udf.BadCase_ScalaUDF13 RETURNS DECIMAL USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF14 AS com.snappy.scala.poc.udf.ScalaUDF14 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF15 AS com.snappy.scala.poc.udf.ScalaUDF15 RETURNS INTEGER USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF16 AS com.snappy.scala.poc.udf.ScalaUDF16 RETURNS BOOLEAN USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF17 AS com.snappy.scala.poc.udf.ScalaUDF17 RETURNS LONG USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF18 AS com.snappy.scala.poc.udf.ScalaUDF18 RETURNS DATE USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF19 AS com.snappy.scala.poc.udf.ScalaUDF19 RETURNS DOUBLE USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF20 AS com.snappy.scala.poc.udf.ScalaUDF20 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF21 AS com.snappy.scala.poc.udf.ScalaUDF21 RETURNS FLOAT USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF22 AS com.snappy.scala.poc.udf.ScalaUDF22 RETURNS INTEGER USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
        } catch (SQLException se) {
            throw new TestException(se.getMessage() + ", Error in creating Scala UDF.");
        }
    }

    private void executeScalaUDF(Connection jdbcConnection) {
            getUDFResult(jdbcConnection,"SELECT ScalaUDF1('snappyData');","UDF1");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF2(CAST('2019-02-17 08:10:15' AS TIMESTAMP),CAST('2019-02-18 10:10:15' AS TIMESTAMP));","UDF2");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF2(CAST('2019-02-17 08:10:15' AS TIMESTAMP),CAST('2019-02-18 10:20:23' AS TIMESTAMP));","UDF2");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF3(15.3d,15.3d,15.3d);","UDF3");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF4(1024,2048,4096,8192);","UDF4");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF5(10,20,30,40,Struct(17933,54.68d,'SnappyData',null));","UDF5");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF6(Map('Maths',98.7d),Map('Science',96.1d),Map('English',89.5d),Map('Social Studies',88.0d),Map('Computer',95.4d),Map('Music',92.5d));","UDF6");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF7(Array(29.8d,30.7d,12.3d,25.9d),Array(17.3d,45.3d,32.6d,24.1d,10.7d,23.8d,78.8d),Array(0.0d,12.3d,33.6d,65.9d,78.9d,21.1d),Array(12.4d,99.9d),Array(23.4d,65.6d,52.1d,32.6d,85.6d),Array(25.6d,52.3d,87.4d),Array(30.2d));","UDF7");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF8('Spark','Snappy','GemXD','Docker','AWS','Scala','JIRA','Git');","UDF8");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF9(false,false,true,false,false,true,true,true,false);","UDF9");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF9(false,false,true,false,false,true,true,true,true);","UDF9");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF10(2,4,6,8,10,12,14,16,18,20);","UDF10");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF11(5.1f,5.2f,5.3f,5.4f,5.5f,5.6f,5.7f,5.8f,5.9f,6.3f,8.7f);","UDF11");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF12('+',15,30,'-',30L,15L,'*',10.5f,10.5F,'/',smallint(12),smallint(12));","UDF12");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF13(123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45);","UDF13");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF13_1(123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45);","UDF13_1");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF14('s','n','a','p','p','y','d','a','t','a','a','s','D','B');","UDF14");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF15(true,false,true,false,true,false,true,false,true,false,true,false,true,false,true);","UDF15");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF15(true,true,true,true,true,true,true,true,true,false,false,false,false,false,true);","UDF15");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF16(TINYINT(1),TINYINT(2),TINYINT(3),TINYINT(4),TINYINT(5),TINYINT(6),TINYINT(7),TINYINT(8),TINYINT(9),TINYINT(10),TINYINT(11),TINYINT(12),TINYINT(13),TINYINT(14),TINYINT(15),TINYINT(16));","UDF16");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF16(TINYINT(2),TINYINT(2),TINYINT(3),TINYINT(4),TINYINT(5),TINYINT(6),TINYINT(7),TINYINT(8),TINYINT(9),TINYINT(10),TINYINT(11),TINYINT(12),TINYINT(13),TINYINT(14),TINYINT(15),TINYINT(16));","UDF16");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF17(125401L,20456789L,1031425L,2000000L,787321654L,528123085L,14777777L,33322211145L,4007458725L,27712345678L,666123654L,1005L,201020092008L,1500321465L,888741852L,963852147L,444368417L);","UDF17");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF18(125L,204L,103L,20L,787L,528L,147L,333L,400L,277L,666L,1005L,2010L,1500L,888L,963L,444L,777L);","UDF18");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF19(25.3d,200.8d,101.5d,201.9d,789.85d,522.398d,144.2d,336.1d,400.0d,277.7d,666.6d,1005.630d,2010.96d,1500.21d,888.7d,963.87d,416.0d,786.687d,1.1d);","UDF19");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF20(' Snappy ','   Data   ','  is  ',' the ','  first    ','   product','to ',' integrate   ','a',' database','  into  ','  spark  ','making      ','   Spark   ','  work  ','just    ','like','    a','         database   ',' . ');","UDF20");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF21('23','21','30','37','25','22','28','32','31','24','24','26','35','89','98','45','54','95','74','66','5');","UDF21");
            getUDFResult(jdbcConnection,"SELECT ScalaUDF22(2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2);","UDF22");
    }

    private void dropScalaUDF(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF1;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF2;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF3;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF4;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF5;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF6;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF7;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF8;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF9;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF10;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF11;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF12;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF13;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF13_1;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF14;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF15;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF16;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF17;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF18;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF19;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF20;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF21;" );
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF22;" );
        }catch (SQLException se) {
            throw new TestException(se.getMessage() + ", Error in dropping Scala UDF.");
        }
    }

    private void create_execute_drop_JavaUDF(Connection jdbcConnection) {
        createTables(jdbcConnection);
        createJavaUDF(jdbcConnection);
        executeJavaUDF(jdbcConnection);
        dropJavaUDF(jdbcConnection);
        dropTables(jdbcConnection);
        closeConnection(jdbcConnection);
        Log.getLogWriter().info("Java UDF execution successful.");
    }

    private void create_execute_drop_ScalaUDF(Connection jdbcConnection) {
        createScalaUDF(jdbcConnection);
        executeScalaUDF(jdbcConnection);
        dropScalaUDF(jdbcConnection);
        closeConnection(jdbcConnection);
        Log.getLogWriter().info("Scala UDF execution successful.");
    }

    public static  void HydraTask_JavaUDFS() {
        UDFInterface udfInterface = new UDFInterface();
        Connection jdbcConnection = udfInterface.getJDBCConnection();
        udfInterface.create_execute_drop_JavaUDF(jdbcConnection);
    }

    public static void HydraTask_ScalaUDFS() {
        UDFInterface udfInterface = new UDFInterface();
        Connection jdbcConnection = udfInterface.getJDBCConnection();
        udfInterface.create_execute_drop_ScalaUDF(jdbcConnection);
    }
}
