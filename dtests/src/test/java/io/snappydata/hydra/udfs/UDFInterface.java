package io.snappydata.hydra.udfs;

import hydra.Log;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.test.util.TestException;
import java.sql.*;
import java.util.Vector;

public class UDFInterface extends SnappyTest {

    //String udfjarPath = snappyTest.getUserAppJarLocation(userAppJar,"");

    public static  void HydraTask_JavaUDFS() {
        UDFInterface udfInterface = new UDFInterface();
        Connection jdbcConnection = udfInterface.getJDBCConnection();
        Log.getLogWriter().info("UDF JAR PATH : " + snappyTest.getUserAppJarLocation(SnappyPrms.getUserAppJar(),""));
         udfInterface.create_execute_drop_JavaUDF(jdbcConnection, snappyTest.getUserAppJarLocation(SnappyPrms.getUserAppJar(),""));
    }

    public static void HydraTask_ScalaUDFS() {
        UDFInterface udfInterface = new UDFInterface();
        Connection jdbcConnection = udfInterface.getJDBCConnection();
        udfInterface.create_execute_drop_ScalaUDF(jdbcConnection, snappyTest.getUserAppJarLocation(SnappyPrms.getUserAppJar(),""));
    }

    public static void HydraTask_Validate_SNAP2658() throws SQLException {
        UDFInterface udfInterface = new UDFInterface();
        Connection jdbcConnection = udfInterface.getJDBCConnection();
        Vector jarLocation =  SnappyPrms.getDataLocationList();
        String  jarPath1 = jarLocation.get(0).toString() + "/UDF1/TestUDF.jar";
        String jarPath2 = jarLocation.get(0).toString() + "/UDF2/TestUDF.jar";
        Log.getLogWriter().info("Jar 1: " + jarPath1);
        Log.getLogWriter().info("Jar 2: " + jarPath2);
        String jar = "";
        for(int count =0; count < 2; count++) {
            if(count == 0)
                jar = "CREATE FUNCTION MyUDF AS io.snappydata.hydra.TestUDF1 RETURNS INTEGER USING JAR '" + jarPath1 + "'";
            else if(count ==1)
                jar = "CREATE FUNCTION MyUDF AS io.snappydata.hydra.TestUDF1 RETURNS INTEGER USING JAR '" + jarPath2 + "'";
            udfInterface.validate_SNAP2658(jdbcConnection, count, jar);
        }
    }




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

    private void validate_SNAP2658(Connection jdbcConnection, int count, String query) throws SQLException {
        ResultSet rs = null;
        Statement st = null;
        String result1 = "";
        String result2 = "";
        Log.getLogWriter().info(query);
        st = jdbcConnection.createStatement();
        jdbcConnection.createStatement().execute(query);
        if(count == 0)
            if(st.execute("select MyUDF(100)")) {
                rs = st.getResultSet();
                ResultSetMetaData rsm = rs.getMetaData();
                String columnName = rsm.getColumnName(1);
                while (rs.next()) {
                    result1 = rs.getString(columnName);
                }
                Log.getLogWriter().info("result1: " + result1);
            }
        if(count == 1)
            if(st.execute("select MyUDF(1000)")) {
                rs = st.getResultSet();
                ResultSetMetaData rsm = rs.getMetaData();
                String columnName = rsm.getColumnName(1);
                while (rs.next()) {
                    result2 = rs.getString(columnName);
                }
                Log.getLogWriter().info("result2: " + result2);
            }
         st.clearBatch();
        jdbcConnection.createStatement().execute("Drop Function MyUDF");
         if(count ==1)
             closeConnection(jdbcConnection);
    }

    private void create_execute_drop_JavaUDF(Connection jdbcConnection, String jarPath) {
        executeJavaUDF1(jdbcConnection, jarPath);
        executeJavaUDF2(jdbcConnection, jarPath);
        executeJavaUDF3(jdbcConnection, jarPath);
        executeJavaUDF4(jdbcConnection, jarPath);
        executeJavaUDF5(jdbcConnection, jarPath);
        executeJavaUDF6(jdbcConnection, jarPath);
        executeJavaUDF7(jdbcConnection, jarPath);
        executeJavaUDF8(jdbcConnection, jarPath);
        executeJavaUDF9(jdbcConnection, jarPath);
        executeJavaUDF10(jdbcConnection, jarPath);
        executeJavaUDF11(jdbcConnection, jarPath);
        executeJavaUDF11_1(jdbcConnection, jarPath);
        executeJavaUDF12(jdbcConnection, jarPath);
        executeJavaUDF13(jdbcConnection, jarPath);
        executeJavaUDF14(jdbcConnection, jarPath);
        executeJavaUDF15(jdbcConnection, jarPath);
        executeJavaUDF16(jdbcConnection, jarPath);
        executeJavaUDF17(jdbcConnection, jarPath);
        executeJavaUDF18(jdbcConnection, jarPath);
        executeJavaUDF19(jdbcConnection, jarPath);
        executeJavaUDF20(jdbcConnection, jarPath);
        executeJavaUDF21(jdbcConnection, jarPath);
        executeJavaUDF22(jdbcConnection, jarPath);
        closeConnection(jdbcConnection);
        Log.getLogWriter().info("Java UDF execution successful.");
    }

    private void create_execute_drop_ScalaUDF(Connection jdbcConnection, String jarPath) {
        executeScalaUDF1(jdbcConnection, jarPath);
        executeScalaUDF2(jdbcConnection, jarPath);
        executeScalaUDF2_1(jdbcConnection, jarPath);
        executeScalaUDF3(jdbcConnection, jarPath);
        executeScalaUDF4(jdbcConnection, jarPath);
        executeScalaUDF5(jdbcConnection, jarPath);
        executeScalaUDF6(jdbcConnection, jarPath);
        executeScalaUDF7(jdbcConnection, jarPath);
        executeScalaUDF8(jdbcConnection, jarPath);
        executeScalaUDF9(jdbcConnection, jarPath);
        executeScalaUDF9_1(jdbcConnection, jarPath);
        executeScalaUDF10(jdbcConnection, jarPath);
        executeScalaUDF11(jdbcConnection, jarPath);
        executeScalaUDF12(jdbcConnection, jarPath);
        executeScalaUDF13(jdbcConnection, jarPath);
        executeScalaUDF14(jdbcConnection, jarPath);
        executeScalaUDF15(jdbcConnection, jarPath);
        executeScalaUDF15_1(jdbcConnection, jarPath);
        executeScalaUDF16(jdbcConnection, jarPath);
        executeScalaUDF16_1(jdbcConnection, jarPath);
        executeScalaUDF17(jdbcConnection, jarPath);
        executeScalaUDF18(jdbcConnection, jarPath);
        executeScalaUDF19(jdbcConnection, jarPath);
        executeScalaUDF20(jdbcConnection, jarPath);
        executeScalaUDF21(jdbcConnection, jarPath);
        executeScalaUDF22(jdbcConnection, jarPath);
        executeScalaUDF13_BadCase(jdbcConnection, jarPath);
        closeConnection(jdbcConnection);
        Log.getLogWriter().info("Scala UDF execution successful.");
    }

    private String getUDFResult(Connection jdbcConnection, String sqlStatement, String udfIndex) {
        ResultSet rs = null;
        Statement st = null;
        String udfType = "Java";
        String result = "";

        try {
            if(sqlStatement.contains("Scala" ))
                udfType = "Scala";
            st = jdbcConnection.createStatement();
            if(st.execute(sqlStatement)) {
                rs = st.getResultSet();
                ResultSetMetaData rsm = rs.getMetaData();
                String columnName = rsm.getColumnName(1);
                while(rs.next()) {
                    result = rs.getString(columnName);
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
            if(udfIndex == "UDF13_1")
                Log.getLogWriter().info("Error in executing UDF13_1 -> " + se.getMessage());
            else
                throw new TestException(se.getMessage() + ", Error in getting UDF result");
        }
        return result;
    }

    private void executeJavaUDF1(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T1;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T1(id Integer) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T1 VALUES(25);");
//            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF1 AS com.snappy.poc.udf.JavaUDF1 RETURNS INTEGER USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF1 AS com.snappy.poc.udf.JavaUDF1 RETURNS INTEGER USING JAR '"+ jarPath + "';");
            result = getUDFResult(jdbcConnection,"SELECT UDF1(id) FROM T1;","UDF1");
            assert result.equals("125")  : "Result mismatch in Java UDF1 ->  Expected : 125, Actual : " + result;
            Log.getLogWriter().info("Java UDF1 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF1;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T1;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF1");
        }
    }

    private void executeJavaUDF2(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T2;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T2(s1 String, s2 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T2 VALUES('Snappy','Data');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF2 AS com.snappy.poc.udf.JavaUDF2 RETURNS STRING USING JAR '" + jarPath + "';");
            result = getUDFResult(jdbcConnection,"SELECT UDF2(s1,s2) FROM T2;","UDF2");
            assert result.equals("Snappy#Data") : "Result mismatch in Java UDF2 ->  Expected : Snappy#Data, Actual : " + result;
            Log.getLogWriter().info("Java UDF2 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF2;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T2;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF2");
        }
    }

    private void executeJavaUDF3(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T3;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T3(arr1 Array<String>,arr2 Array<String>,arr3 Array<String>) USING COLUMN;");
            //jdbcConnection.createStatement().execute("INSERT INTO T3 VALUES(array('snappydata','udf1testing','successful'),array('snappydata','udf2testing','successful'),array('snappydata','udf3testing','successful'));");
            jdbcConnection.createStatement().execute("INSERT INTO T3 SELECT Array('snappydata','udf1testing','successful'),Array('snappydata','udf2testing','successful'),Array('snappydata','udf3testing','successful');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF3 AS com.snappy.poc.udf.JavaUDF3 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result = getUDFResult(jdbcConnection,"SELECT UDF3(arr1,arr2,arr3) FROM T3;","UDF3");
            assert result.equals("SNAPPYDATA UDF1TESTING SUCCESSFUL SNAPPYDATA UDF2TESTING SUCCESSFUL SNAPPYDATA UDF3TESTING SUCCESSFUL") : "Result mismatch in Java UDF3 -> " +
                    " Expected : SNAPPYDATA UDF1TESTING SUCCESSFUL SNAPPYDATA UDF2TESTING SUCCESSFUL SNAPPYDATA UDF3TESTING SUCCESSFUL : , Actual : " + result;
            Log.getLogWriter().info("Java UDF3 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF3;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T3;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF3");
        }
    }

    private void executeJavaUDF4(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T4;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T4(m1 Map<String,Double>,m2 Map<String,Double>,m3 Map<String,Double>,m4 Map<String,Double>) USING COLUMN");
            jdbcConnection.createStatement().execute("INSERT INTO T4 SELECT Map('Maths',100.0d),Map('Science',96.5d),Map('English',92.0d),Map('Music',98.5d);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF4 AS com.snappy.poc.udf.JavaUDF4 RETURNS DOUBLE USING JAR '" +  jarPath+ "';");
            result= getUDFResult(jdbcConnection,"SELECT UDF4(m1,m2,m3,m4) FROM T4;","UDF4");
            assert result.equals("387.0") : "Result mismatch in Java UDF4 ->  Expected : 387.0, Actual : " + result;
            Log.getLogWriter().info("Java UDF4 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF4;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T4;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF4");
        }
    }

    private void executeJavaUDF5(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T5;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T5(b1 Boolean,b2 Boolean,b3 Boolean,b4 Boolean,b5 Boolean) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T5 VALUES(true,true,true,true,true);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF5 AS com.snappy.poc.udf.JavaUDF5 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result = getUDFResult(jdbcConnection,"SELECT UDF5(b1,b2,b3,b4,b5) FROM T5;","UDF5");
            assert result.equals("0x:1f") : "Result mismatch in Java UDF5 ->  Expected : 0x:1f, Actual : " + result;
            Log.getLogWriter().info("Java UDF5 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF5;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T5;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF5");
        }
    }

    private void executeJavaUDF6(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T6;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T6(d1 Decimal,d2 Decimal,d3 Decimal,d4 Decimal,d5 Decimal,d6 Decimal) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T6 VALUES(231.45,563.93,899.25,611.98,444.44,999.99);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF6 AS com.snappy.poc.udf.JavaUDF6 RETURNS DECIMAL USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF6(d1,d2,d3,d4,d5,d6) FROM T6;","UDF6");
            assert result.equals("3751.040000000000000000") : "Result mismatch in Java UDF6 ->  Expected : 3751.040000000000000000, Actual : " + result;
            Log.getLogWriter().info("Java UDF6 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF6;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T6;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF6");
        }
    }

    private void executeJavaUDF7(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T7;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T7(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer,st Struct<ist:Integer,dst:Double,sst:String,snull:Integer>) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T7 SELECT 10,20,30,40,50,60,Struct(17933,54.68d,'SnappyData','');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF7 AS com.snappy.poc.udf.JavaUDF7 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF7(i1,i2,i3,i4,i5,i6,st) FROM T7;","UDF7");
            assert result.equals("210(17933,54.68,SnappyData, IsNull :true)")  : "Result mismatch in Java UDF7 ->  Expected : 210(17933,54.68,SnappyData, IsNull :true), Actual : " + result;
            Log.getLogWriter().info("Java UDF7 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF7;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T7;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF7");
        }
    }

    private void executeJavaUDF8(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T8;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T8(l1 Long,l2 Long,l3 Long,l4 Long,l5 Long,l6 Long,l7 Long,l8 Long) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T8 VALUES(10,20,30,40,50,60,70,80);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF8 AS com.snappy.poc.udf.JavaUDF8 RETURNS SHORT USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF8(l1,l2,l3,l4,l5,l6,l7,l8) FROM T8;","UDF8");
            assert result.equals("8")  : "Result mismatch in Java UDF8 ->  Expected : 8, Actual : " + result;
            Log.getLogWriter().info("Java UDF8 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF8;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T8;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF8");
        }
    }

    private void executeJavaUDF9(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T9;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T9(c1 Varchar(9),c2 Varchar(9),c3 Varchar(9),c4 Varchar(9),c5 Varchar(9),c6 Varchar(9),c7 Varchar(9),c8 Varchar(9),c9 Varchar(9)) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T9 VALUES('c','h','a','n','d','r','e','s','h');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF9 AS com.snappy.poc.udf.JavaUDF9 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF9(c1,c2,c3,c4,c5,c6,c7,c8,c9) FROM T9;","UDF9");
            assert result.equals("chandresh")  : "Result mismatch in Java UDF9 ->  Expected : chandresh, Actual : " + result;
            Log.getLogWriter().info("Java UDF9 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF9;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T9;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF9");
        }
    }

    private void executeJavaUDF10(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T10;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T10(f1 Float,f2 Float,f3 Float,f4 Float,f5 Float,f6 Float,f7 Float,f8 Float,f9 Float,f10 Float) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T10 VALUES(12.7,12.7,12.7,12.7,12.7,12.7,12.7,12.7,12.7,12.7);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF10 AS com.snappy.poc.udf.JavaUDF10 RETURNS FLOAT USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF10(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10) FROM T10;","UDF10");
            assert result.equals("126.999985")  : "Result mismatch in Java UDF10 ->  Expected : 126.999985, Actual : " + result;
            Log.getLogWriter().info("Java UDF10 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF10;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T10;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF10");
        }
    }

    private void executeJavaUDF11(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T11;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T11(f1 Float,f2 Float,f3 Float,f4 Float,f5 Float,f6 Float,f7 Float,f8 Float,f9 Float,f10 Float,f11 Float) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T11 VALUES(12.1,12.2,12.3,12.4,12.5,12.6,12.7,12.8,12.9,14.1,12.8);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF11 AS com.snappy.poc.udf.JavaUDF11 RETURNS BOOLEAN USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF11(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11) FROM T11;","UDF11");
            assert result.equals("true")  : "Result mismatch in Java UDF11 ->  Expected : true, Actual : " + result;
            Log.getLogWriter().info("Java UDF11 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF11;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T11;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF11");
        }
    }

    private void executeJavaUDF11_1(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T11;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T11(f1 Float,f2 Float,f3 Float,f4 Float,f5 Float,f6 Float,f7 Float,f8 Float,f9 Float,f10 Float,f11 Float) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T11 VALUES(12.1,12.2,12.3,12.4,12.5,12.6,12.7,12.8,12.9,14.1,15.5);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF11 AS com.snappy.poc.udf.JavaUDF11 RETURNS BOOLEAN USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF11(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11) FROM T11;","UDF11");
            assert result.equals("false")  : "Result mismatch in Java UDF11_1 ->  Expected : false, Actual : " + result;
            Log.getLogWriter().info("Java UDF11_1 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF11;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T11;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF11");
        }
    }

    private void executeJavaUDF12(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T12;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T12(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String,s9 String,s10 String,s11 String,s12 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T12 VALUES('Snappy','Data','Leader','Locator','Server','Cluster','Performance','UI','SQL','DataFrame','Dataset','API');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF12 AS com.snappy.poc.udf.JavaUDF12 RETURNS DATE USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF12(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12) FROM T12;","UDF12");
            Log.getLogWriter().info("Java UDF12 -> " + result);
            assert result.equals(new Date(System.currentTimeMillis()).toString())  : "Result mismatch in Java UDF12 ->  Expected : " + new Date(System.currentTimeMillis()).toString() + ", Actual : " + result;
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF12;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T12;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF12");
        }
    }

    private void executeJavaUDF13(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T13;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T13(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer,i7 Integer,i8 Integer,i9 Integer,i10 Integer,i11 Integer,i12 Integer,i13 Integer) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T13 VALUES(10,21,25,54,89,78,63,78,55,13,14,85,71);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF13 AS com.snappy.poc.udf.JavaUDF13 RETURNS TIMESTAMP USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF13(i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13) FROM T13;","UDF13");
            assert result.equals(new Timestamp(System.currentTimeMillis()).toString())  : "Result mismatch in Java UDF13 ->  Expected :" + new Timestamp(System.currentTimeMillis()).toString() +", Actual : " + result;
            Log.getLogWriter().info("Java UDF13 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF13;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T13;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF13");
        }
    }

    private void executeJavaUDF14(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T14;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T14(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String,s9 String,s10 String,s11 String,s12 String,s13 String,s14 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T14 VALUES('0XFF','10','067','54534','0X897','999','077','1234567891234567','0x8978','015','1005','13177','14736','07777');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF14 AS com.snappy.poc.udf.JavaUDF14 RETURNS LONG USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF14(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14) FROM T14;","UDF14");
            assert result.equals("10")  : "Result mismatch in Java UDF14 ->  Expected : 10, Actual : " + result;
            Log.getLogWriter().info("Java UDF14 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF14;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T14;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF14");
        }
    }

    private void executeJavaUDF15(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T15;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T15(d1 Double,d2 Double,d3 Double,d4 Double,d5 Double,d6 Double,d7 Double,d8 Double,d9 Double,d10 Double,d11 Double,d12 Double,d13 Double,d14 Double,d15 Double) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T15 VALUES(5.23,99.3,115.6,0.9,78.5,999.99,458.32,133.58,789.87,269.21,333.96,653.21,550.32,489.14,129.87);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF15 AS com.snappy.poc.udf.JavaUDF15 RETURNS DOUBLE USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF15(d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15) FROM T15;","UDF15");
            assert result.equals("999.99")  : "Result mismatch in Java UDF15 ->  Expected : 999.99, Actual : " + result;
            Log.getLogWriter().info("Java UDF15 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF15;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T15;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF15");
        }
    }

    private void executeJavaUDF16(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T16;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T16(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String,s9 String,s10 String,s11 String,s12 String,s13 String,s14 String,s15 String,s16 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T16 VALUES('snappy','data','is','working','on','a','distributed','data','base.','Its','Performance','is','faster','than','Apache','spark');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF16 AS com.snappy.poc.udf.JavaUDF16 RETURNS VARCHAR(255) USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF16(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,s15,s16) FROM T16;","UDF16");
            assert result.equals("snappy+data+is+working+on+a+distributed+data+base.+Its+Performance+is+faster+than+Apache+spark")  : "Result mismatch in Java UDF16 ->  Expected : snappy+data+is+working+on+a+distributed+data+base.+Its+Performance+is+faster+than+Apache+spark, Actual : " + result;
            Log.getLogWriter().info("Java UDF16 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF16;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T16;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF16");
        }
    }

    private void executeJavaUDF17(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T17;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T17(f1 Float,f2 Float,f3 Float,f4 Float,f5 Float,f6 Float,f7 Float,f8 Float,f9 Float,f10 Float,f11 Float,f12 Float,f13 Float,f14 Float,f15 Float,f16 Float,f17 Float) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T17 VALUES(15.2,17.2,99.9,45.2,71.0,89.8,66.6,23.7,33.1,13.5,77.7,38.3,44.4,21.7,41.8,83.2,78.1);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF17 AS com.snappy.poc.udf.JavaUDF17 RETURNS BOOLEAN USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF17(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17) FROM T17;","UDF17");
            assert result.equals("true")  : "Result mismatch in Java UDF17 ->  Expected : true, Actual : " + result;
            Log.getLogWriter().info("Java UDF17 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF17;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T17;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF17");
        }
    }

    private void executeJavaUDF18(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T18;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T18(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String,s9 String,s10 String,s11 String,s12 String,s13 String,s14 String,s15 String,s16 String,s17 String,s18 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T18 VALUES('snappy','data','spark','leader','locator','server','memory','eviction','sql','udf','udaf','scala','docker','catalog','smart connector','cores','executor','dir');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF18 AS com.snappy.poc.udf.JavaUDF18 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF18(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,s15,s16,s17,s18) FROM T18;","UDF18");
            assert result.equals("executorsmart connectorcatalogdockerevictionmemoryserverlocatorleadersnappy,,,,,,,,,,")  : "Result mismatch in Java UDF18 ->  Expected : executorsmart connectorcatalogdockerevictionmemoryserverlocatorleadersnappy,,,,,,,,,,, Actual : " + result;
            Log.getLogWriter().info("Java UDF18 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF18;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T18;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF18");
        }
    }

    private void executeJavaUDF19(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T19;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T19(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String,s9 String,s10 String,s11 String,s12 String,s13 String,s14 String,s15 String,s16 String,s17 String,s18 String,s19 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T19 VALUES('snappy','data','spark','leader','locator','server','memory','eviction','sql','udf','udaf','scala','docker','catalog','smart connector','cores','executor','dir','avro');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF19 AS com.snappy.poc.udf.JavaUDF19 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF19(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,s15,s16,s17,s18,s19) FROM T19;","UDF19");
            assert result.equals("SNAPPY,DATA,SPARK,LEADER,LOCATOR,SERVER,MEMORY,EVICTION,SQL,UDF,UDAF,SCALA,DOCKER,CATALOG,SMART CONNECTOR,CORES,EXECUTOR,DIR,AVRO,")  : "Result mismatch in Java UDF19 ->  Expected : SNAPPY,DATA,SPARK,LEADER,LOCATOR,SERVER,MEMORY,EVICTION,SQL,UDF,UDAF,SCALA,DOCKER,CATALOG,SMART CONNECTOR,CORES,EXECUTOR,DIR,AVRO,, Actual : " + result;
            Log.getLogWriter().info("Java UDF19 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF19;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T19;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF19");
        }
    }

    private void executeJavaUDF20(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T20;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T20(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer,i7 Integer,i8 Integer,i9 Integer,i10 Integer,i11 Integer,i12 Integer,i13 Integer,i14 Integer,i15 Integer,i16 Integer,i17 Integer,i18 Integer,i19 Integer,i20 Integer) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T20 VALUES(10,20,30,40,50,60,70,80,90,100,13,17,21,25,19,22,86,88,64,58);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF20 AS com.snappy.poc.udf.JavaUDF20 RETURNS INTEGER USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF20(i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15,i16,i17,i18,i19,i20) FROM T20;","UDF20");
            assert result.equals("15")  : "Result mismatch in Java UDF20 ->  Expected : 15, Actual : " + result;
            Log.getLogWriter().info("Java UDF20 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF20;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T20;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF20");
        }
    }

    private void executeJavaUDF21(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T21;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T21(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String,s9 String,s10 String,s11 String,s12 String,s13 String,s14 String,s15 String,s16 String,s17 String,s18 String,s19 String,s20 String,s21 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T21 VALUES('snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF21 AS com.snappy.poc.udf.JavaUDF21 RETURNS INTEGER USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF21(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,s15,s16,s17,s18,s19,s20,s21) FROM T21;","UDF21");
            assert result.equals("126")  : "Result mismatch in Java UDF21 ->  Expected : 126, Actual : " + result;
            Log.getLogWriter().info("Java UDF21 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF21;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T21;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF21");
        }
    }

    private void executeJavaUDF22(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T22;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T22(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer,i7 Integer,i8 Integer,i9 Integer,i10 Integer,i11 Integer,i12 Integer,i13 Integer,i14 Integer,i15 Integer,i16 Integer,i17 Integer,i18 Integer,i19 Integer,i20 Integer,i21 Integer,i22 Integer) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T22 VALUES(10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF22 AS com.snappy.poc.udf.JavaUDF22 RETURNS INTEGER USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT UDF22(i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15,i16,i17,i18,i19,i20,i21,i22) FROM T22;","UDF22");
            assert result.equals("220")  : "Result mismatch in Java UDF22 ->  Expected : 220, Actual : " + result;
            Log.getLogWriter().info("Java UDF22 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF22;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T22;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF22");
        }
    }

    private void executeScalaUDF1(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST1;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST1(s1 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST1 VALUES('snappyData');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF1 AS com.snappy.scala.poc.udf.ScalaUDF1 RETURNS TIMESTAMP USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF1(s1) FROM ST1;","UDF1");
            assert result.equals(new Timestamp(System.currentTimeMillis()).toString())  : "Result mismatch in Scala UDF1 ->  Expected : new Timestamp(System.currentTimeMillis()).toString(), Actual : " + result;
            Log.getLogWriter().info("Scala UDF1 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF1;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST1;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF1");
        }
    }

    private void executeScalaUDF2(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST2;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST2(ts1 TimeStamp,ts2 TimeStamp) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST2 VALUES(CAST('2019-02-17 08:10:15' AS TIMESTAMP),CAST('2019-02-18 10:10:15' AS TIMESTAMP));");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF2 AS com.snappy.scala.poc.udf.ScalaUDF2 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF2(ts1,ts2) FROM ST2;","UDF2");
            assert result.equals("Difference is :\n" +
                    "Hours : 26\n" +
                    "Minutes : 0\n" +
                    "Seconds : 0")  : "Result mismatch in Scala UDF2 ->  Expected : Difference is :\n" +
                    "Hours : 26\n" +
                    "Minutes : 0\n" +
                    "Seconds : 0, Actual : " + result;
            Log.getLogWriter().info("Scala UDF2 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF2;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST2;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF2");
        }
    }

    private void executeScalaUDF2_1(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST2;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST2(ts1 TimeStamp,ts2 TimeStamp) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST2 VALUES(CAST('2019-02-17 08:10:15' AS TIMESTAMP),CAST('2019-02-18 10:20:23' AS TIMESTAMP));");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF2 AS com.snappy.scala.poc.udf.ScalaUDF2 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF2(ts1,ts2) FROM ST2;","UDF2");
            assert result.equals("Difference is :\n" +
                    "Hours : 26\n" +
                    "Minutes : 10\n" +
                    "Seconds : 8")  : "Result mismatch in Scala UDF2_1 ->  Expected : Difference is :\n" +
                    "Hours : 26\n" +
                    "Minutes : 10\n" +
                    "Seconds : 8, Actual : " + result;
            Log.getLogWriter().info("Scala UDF2_1 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF2;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST2;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF2");
        }
    }

    private void executeScalaUDF3(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST3;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST3(d1 Double,d2 Double,d3 Double) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST3 VALUES(15.3,15.3,15.3);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF3 AS com.snappy.scala.poc.udf.ScalaUDF3 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF3(d1,d2,d3) FROM ST3;","UDF3");
            assert result.equals("Sine :0.39674057313061206\n" +
                    "Cosine : -0.9179307804142932\n" +
                    "Tangent : -0.4322118634604993")  : "Result mismatch in Scala UDF3 ->  Expected : ine :0.39674057313061206\n" +
                    "Cosine : -0.9179307804142932\n" +
                    "Tangent : -0.4322118634604993, Actual : " + result;
            Log.getLogWriter().info("Scala UDF3 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF3;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST3;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF3");
        }
    }

    private void executeScalaUDF4(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST4;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST4(i1 Integer,i2 Integer,i3 Integer,i4 Integer) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST4 VALUES(1024,2048,4096,8192);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF4 AS com.snappy.scala.poc.udf.ScalaUDF4 RETURNS DOUBLE USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF4(i1,i2,i3,i4) FROM ST4;","UDF4");
            assert result.equals("231.76450198781714")  : "Result mismatch in Scala UDF4 ->  Expected : 231.76450198781714, Actual : " + result;
            Log.getLogWriter().info("Scala UDF4 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF4;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST4;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF4");
        }
    }

    private void executeScalaUDF5(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST5;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST5(i1 Integer,i2 Integer,i3 Integer,i4 Integer,st Struct<ist:Integer,dst:Double,sst:String,snull:Integer>) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST5 SELECT 10,20,30,40,Struct(17933,54.68,'SnappyData','');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF5 AS com.snappy.scala.poc.udf.ScalaUDF5 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF5(i1,i2,i3,i4,st) FROM ST5;","UDF5");
            assert result.equals("Sum of four integer : 100, Struct data type element : (17933,54.68,SnappyData,true)")  : "Result mismatch in Scala UDF5 ->  Expected : Sum of four integer : 100, Struct data type element : (17933,54.68,SnappyData,true), Actual : " + result;
            Log.getLogWriter().info("Scala UDF5 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF5;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST5;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF5");
        }
    }

    private void executeScalaUDF6(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST6;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST6(m1 Map<String,Double>,m2 Map<String,Double>,m3 Map<String,Double>,m4 Map<String,Double>,m5 Map<String,Double>, m6 Map<String,Double>) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST6 SELECT Map('Maths',98.7),Map('Science',96.1),Map('English',89.5),Map('Social Studies',88.0),Map('Computer',95.4),Map('Music',92.5);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF6 AS com.snappy.scala.poc.udf.ScalaUDF6 RETURNS DOUBLE USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF6(m1,m2,m3,m4,m5,m6) FROM ST6;","UDF6");
            assert result.equals("560.2")  : "Result mismatch in Scala UDF6 ->  Expected : 560.2, Actual : " + result;
            Log.getLogWriter().info("Scala UDF6 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF6;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST6;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF6");
        }
    }

    private void executeScalaUDF7(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST7;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST7(arr1 Array<Double>,arr2 Array<Double>,arr3 Array<Double>,arr4 Array<Double>,arr5 Array<Double>,arr6 Array<Double>,arr7 Array<Double>) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST7 SELECT Array(29.8,30.7,12.3,25.9),Array(17.3,45.3,32.6,24.1,10.7,23.8,78.8),Array(0.0,12.3,33.6,65.9,78.9,21.1),Array(12.4,99.9),Array(23.4,65.6,52.1,32.6,85.6),Array(25.6,52.3,87.4),Array(30.2);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF7 AS com.snappy.scala.poc.udf.ScalaUDF7 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF7(arr1,arr2,arr3,arr4,arr5,arr6,arr7) FROM ST7;","UDF7");
            assert result.equals("V1 : 24.674999999999997\n" +
                    " V2 : 33.22857142857142\n" +
                    " V2 : 35.300000000000004\n" +
                    " V2 : 56.150000000000006\n" +
                    " V2 : 51.85999999999999\n" +
                    " V2 : 55.1\n" +
                    " V2 : 30.2")  : "Result mismatch in Scala UDF7 ->  Expected : V1 : 24.674999999999997\n" +
                    " V2 : 33.22857142857142\n" +
                    " V2 : 35.300000000000004\n" +
                    " V2 : 56.150000000000006\n" +
                    " V2 : 51.85999999999999\n" +
                    " V2 : 55.1\n" +
                    " V2 : 30.2, Actual : " + result;
            Log.getLogWriter().info("Scala UDF7 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF7;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST7;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF7");
        }
    }

    private void executeScalaUDF8(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST8;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST8(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST8 VALUES('Spark','Snappy','GemXD','Docker','AWS','Scala','JIRA','Git');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF8 AS com.snappy.scala.poc.udf.ScalaUDF8 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF8(s1,s2,s3,s4,s5,s6,s7,s8) FROM ST8;","UDF8");
            assert result.equals("Spark->Computation Engine\n" +
                    "Snappy->In-memory database\n" +
                    "GemXD->Storage layer\n" +
                    "Docker->Container Platform\n" +
                    "AWS->Cloud Platform\n" +
                    "Scala->Programming Language\n" +
                    "JIRA->Bug/Task tracking tool\n" +
                    "Git->Version contr&")  : "Result mismatch in Scala UDF8 ->  Expected : Spark->Computation Engine\n" +
                    "Snappy->In-memory database\n" +
                    "GemXD->Storage layer\n" +
                    "Docker->Container Platform\n" +
                    "AWS->Cloud Platform\n" +
                    "Scala->Programming Language\n" +
                    "JIRA->Bug/Task tracking tool\n" +
                    "Git->Version contr&, Actual : " + result;
            Log.getLogWriter().info("Scala UDF8 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF8;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST8;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF8");
        }
    }

    private void executeScalaUDF9(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST9;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST9(b1 Boolean,b2 Boolean,b3 Boolean,b4 Boolean,b5 Boolean,b6 Boolean,b7 Boolean,b8 Boolean,b9 Boolean) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST9 VALUES(false,false,true,false,false,true,true,true,false);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF9 AS com.snappy.scala.poc.udf.ScalaUDF9 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF9(b1,b2,b3,b4,b5,b6,b7,b8,b9) FROM ST9;","UDF9");
            assert result.equals("false && false -> false\n" +
                    "false && true -> false\n" +
                    "true && false -> false\n" +
                    "true && true -> true\n" +
                    "false || false -> false\n" +
                    "false || true -> true\n" +
                    "true || false -> true\n" +
                    "true || true -> true\n" +
                    "!(true) Or !(false) -> true")  : "Result mismatch in Scala UDF9 ->  Expected : false && false -> false\n" +
                    "false && true -> false\n" +
                    "true && false -> false\n" +
                    "true && true -> true\n" +
                    "false || false -> false\n" +
                    "false || true -> true\n" +
                    "true || false -> true\n" +
                    "true || true -> true\n" +
                    "!(true) Or !(false) -> true, Actual : " + result;
            Log.getLogWriter().info("Scala UDF9 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF9;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST9;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF9");
        }
    }

    private void executeScalaUDF9_1(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST9;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST9(b1 Boolean,b2 Boolean,b3 Boolean,b4 Boolean,b5 Boolean,b6 Boolean,b7 Boolean,b8 Boolean,b9 Boolean) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST9 VALUES(false,false,true,false,false,true,true,true,true);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF9 AS com.snappy.scala.poc.udf.ScalaUDF9 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF9(b1,b2,b3,b4,b5,b6,b7,b8,b9) FROM ST9;","UDF9");
            assert result.equals("false && false -> false\n" +
                    "false && true -> false\n" +
                    "true && false -> false\n" +
                    "true && true -> true\n" +
                    "false || false -> false\n" +
                    "false || true -> true\n" +
                    "true || false -> true\n" +
                    "true || true -> true\n" +
                    "!(true) Or !(false) -> false")  : "Result mismatch in Scala UDF9_1 ->  Expected : false && false -> false\n" +
                    "false && true -> false\n" +
                    "true && false -> false\n" +
                    "true && true -> true\n" +
                    "false || false -> false\n" +
                    "false || true -> true\n" +
                    "true || false -> true\n" +
                    "true || true -> true\n" +
                    "!(true) Or !(false) -> false, Actual : " + result;
            Log.getLogWriter().info("Scala UDF9_1 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF9;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST9;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF9");
        }
    }

    private void executeScalaUDF10(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST10;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST10(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer,i7 Integer,i8 Integer,i9 Integer,i10 Integer) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST10 VALUES(2,4,6,8,10,12,14,16,18,20);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF10 AS com.snappy.scala.poc.udf.ScalaUDF10 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF10(i1,i2,i3,i4,i5,i6,i7,i8,i9,i10) FROM ST10;","UDF10");
            assert result.equals("(2,24,720,40320,3628800,479001600,87178291200,20922789888000,6402373705728000,2432902008176640000)")  : "Result mismatch in Scala UDF10 ->  Expected : (2,24,720,40320,3628800,479001600,87178291200,20922789888000,6402373705728000,2432902008176640000), Actual : " + result;
            Log.getLogWriter().info("Scala UDF10 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF10;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST10;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF10");
        }
    }

    private void executeScalaUDF11(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST11;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST11(f1 Float,f2 Float,f3 Float,f4 Float,f5 Float,f6 Float,f7 Float,f8 Float,f9 Float,f10 Float,f11 Float) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST11 VALUES(5.1,5.2,5.3,5.4,5.5,5.6,5.7,5.8,5.9,6.3,8.7);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF11 AS com.snappy.scala.poc.udf.ScalaUDF11 RETURNS FLOAT USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF11(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11) FROM ST11;","UDF11");
            assert result.equals("2.49924368E8")  : "Result mismatch in Scala UDF11 ->  Expected : 2.49924368E8, Actual : " + result;
            Log.getLogWriter().info("Scala UDF11 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF11;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST11;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF11");
        }
    }

    private void executeScalaUDF12(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST12;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST12(s1 String,i1 Integer,i2 Integer,s2 String,l1 Long,l2 Long,s3 String,f1 Float,f2 Float,s4 String, sh1 Short,sh2 Short) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST12 VALUES('+',15,30,'-',30,15,'*',10.5,10.5,'/',smallint(12),smallint(12));");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF12 AS com.snappy.scala.poc.udf.ScalaUDF12 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF12(s1,i1,i2,s2,l1,l2,s3,f1,f2,s4,sh1,sh2) FROM ST12;","UDF12");
            assert result.equals("Addition -> 45 , Substraction -> 15 , Multiplication -> 110.25 , Division -> 1")  : "Result mismatch in Scala UDF12 ->  Expected : Addition -> 45 , Substraction -> 15 , Multiplication -> 110.25 , Division -> 1, Actual : " + result;
            Log.getLogWriter().info("Scala UDF12 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF12;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST12;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF12");
        }
    }

    private void executeScalaUDF13(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST13;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST13(d1 Decimal(5,2),d2 Decimal(5,2),d3 Decimal(5,2),d4 Decimal(5,2),d5 Decimal(5,2),d6 Decimal(5,2),d7 Decimal(5,2),d8 Decimal(5,2),d9 Decimal(5,2),d10 Decimal(5,2),d11 Decimal(5,2),d12 Decimal(5,2),d13 Decimal(5,2)) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST13 VALUES(123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF13 AS com.snappy.scala.poc.udf.ScalaUDF13 RETURNS DECIMAL USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF13(d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13) FROM ST13;","UDF13");
            assert result.equals("160485.000000000000000000")  : "Result mismatch in Scala UDF13 ->  Expected : 160485.000000000000000000, Actual : " + result;
            Log.getLogWriter().info("Scala UDF13 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF13;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST13;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF13");
        }
    }

    private void executeScalaUDF13_BadCase(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST13;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST13(d1 Decimal(5,2),d2 Decimal(5,2),d3 Decimal(5,2),d4 Decimal(5,2),d5 Decimal(5,2),d6 Decimal(5,2),d7 Decimal(5,2),d8 Decimal(5,2),d9 Decimal(5,2),d10 Decimal(5,2),d11 Decimal(5,2),d12 Decimal(5,2),d13 Decimal(5,2)) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST13 VALUES(123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45,123.45);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF13_1 AS com.snappy.scala.poc.udf.BadCase_ScalaUDF13 RETURNS DECIMAL USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF13_1(d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13) FROM ST13;","UDF13_1");
            //assert result.equals("")  : "Result mismatch in Scala UDF13_1 ->  Expected : , Actual : " + result;
            Log.getLogWriter().info("Scala UDF13_BadCase -> " + result);
        }catch(SQLException se) {
            Log.getLogWriter().info(se.getMessage() + " Error in executing ScalaUDF13.");
            //throw new TestException(se.getMessage() + " Error in executing ScalaUDF13");
        }finally {
            try {
                jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF13_1;");
                jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST13;");
            } catch(SQLException se) {
                throw new TestException(se.getMessage());
            }
       }

    }

    private void executeScalaUDF14(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST14;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST14(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String,s9 String,s10 String,s11 String,s12 String,s13 String,s14 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST14 VALUES('s','n','a','p','p','y','d','a','t','a','a','s','D','B');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF14 AS com.snappy.scala.poc.udf.ScalaUDF14 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF14(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14) FROM ST14;","UDF14");
            assert result.equals("SnappyData As Spark DB")  : "Result mismatch in Scala UDF14 ->  Expected : SnappyData As Spark DB, Actual : " + result;
            Log.getLogWriter().info("Scala UDF14 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF14;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST14;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF14");
        }
    }

    private void executeScalaUDF15(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST15;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST15(b1 Boolean,b2 Boolean,b3 Boolean,b4 Boolean,b5 Boolean,b6 Boolean,b7 Boolean,b8 Boolean,b9 Boolean,b10 Boolean,b11 Boolean,b12 Boolean,b13 Boolean,b14 Boolean,b15 Boolean) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST15 VALUES(true,false,true,false,true,false,true,false,true,false,true,false,true,false,true);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF15 AS com.snappy.scala.poc.udf.ScalaUDF15 RETURNS INTEGER USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF15(b1,b2,b3,b4,b5,b6,b7,b8,b9,b10,b11,b12,b13,b14,b15) FROM ST15;","UDF15");
            assert result.equals("21845")  : "Result mismatch in Scala UDF15 ->  Expected : 21845, Actual : " + result;
            Log.getLogWriter().info("Scala UDF15 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF15;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST15;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF15");
        }
    }

    private void executeScalaUDF15_1(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST15;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST15(b1 Boolean,b2 Boolean,b3 Boolean,b4 Boolean,b5 Boolean,b6 Boolean,b7 Boolean,b8 Boolean,b9 Boolean,b10 Boolean,b11 Boolean,b12 Boolean,b13 Boolean,b14 Boolean,b15 Boolean) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST15 VALUES(true,true,true,true,true,true,true,true,true,false,false,false,false,false,true);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF15 AS com.snappy.scala.poc.udf.ScalaUDF15 RETURNS INTEGER USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF15(b1,b2,b3,b4,b5,b6,b7,b8,b9,b10,b11,b12,b13,b14,b15) FROM ST15;","UDF15");
            assert result.equals("32705")  : "Result mismatch in Scala UDF15_1 ->  Expected : 32705, Actual : " + result;
            Log.getLogWriter().info("Scala UDF15_1 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF15;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST15;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF15");
        }
    }

    private void executeScalaUDF16(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST16;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST16(b1 Byte,b2 Byte,b3 Byte,b4 Byte,b5 Byte,b6 Byte,b7 Byte,b8 Byte,b9 Byte,b10 Byte,b11 Byte,b12 Byte,b13 Byte,b14 Byte,b15 Byte,b16 Byte) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST16 VALUES(TINYINT(1),TINYINT(2),TINYINT(3),TINYINT(4),TINYINT(5),TINYINT(6),TINYINT(7),TINYINT(8),TINYINT(9),TINYINT(10),TINYINT(11),TINYINT(12),TINYINT(13),TINYINT(14),TINYINT(15),TINYINT(16));");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF16 AS com.snappy.scala.poc.udf.ScalaUDF16 RETURNS BOOLEAN USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF16(b1,b2,b3,b4,b5,b6,b7,b8,b9,b10,b11,b12,b13,b14,b15,b16) FROM ST16;","UDF16");
            assert result.equals("true")  : "Result mismatch in Scala UDF16 ->  Expected : true, Actual : " + result;
            Log.getLogWriter().info("Scala UDF16 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF16;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST16;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF16");
        }
    }

    private void executeScalaUDF16_1(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST16;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST16(b1 Byte,b2 Byte,b3 Byte,b4 Byte,b5 Byte,b6 Byte,b7 Byte,b8 Byte,b9 Byte,b10 Byte,b11 Byte,b12 Byte,b13 Byte,b14 Byte,b15 Byte,b16 Byte) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST16 VALUES(TINYINT(2),TINYINT(2),TINYINT(3),TINYINT(4),TINYINT(5),TINYINT(6),TINYINT(7),TINYINT(8),TINYINT(9),TINYINT(10),TINYINT(11),TINYINT(12),TINYINT(13),TINYINT(14),TINYINT(15),TINYINT(16));");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF16 AS com.snappy.scala.poc.udf.ScalaUDF16 RETURNS BOOLEAN USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF16(b1,b2,b3,b4,b5,b6,b7,b8,b9,b10,b11,b12,b13,b14,b15,b16) FROM ST16;","UDF16");
            assert result.equals("false")  : "Result mismatch in Scala UDF16_1 ->  Expected : false, Actual : " + result;
            Log.getLogWriter().info("Scala UDF16_1 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF16;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST16;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF16");
        }
    }

    private void executeScalaUDF17(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST17;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST17(l1 Long,l2 Long,l3 Long,l4 Long,l5 Long,l6 Long,l7 Long,l8 Long,l9 Long,l10 Long,l11 Long,l12 Long,l13 Long,l14 Long,l15 Long,l16 Long,l17 Long) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST17 VALUES(125401,20456789,1031425,2000000,787321654,528123085,14777777,33322211145,4007458725,27712345678,666123654,1005,201020092008,1500321465,888741852,963852147,444368417);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF17 AS com.snappy.scala.poc.udf.ScalaUDF17 RETURNS LONG USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF17(l1,l2,l3,l4,l5,l6,l7,l8,l9,l10,l11,l12,l13,l14,l15,l16,l17) FROM ST17;","UDF17");
            assert result.equals("271879352227")  : "Result mismatch in Scala UDF17 ->  Expected : 271879352227, Actual : " + result;
            Log.getLogWriter().info("Scala UDF17 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF17;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST17;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF17");
        }
    }

    private void executeScalaUDF18(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST18;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST18(l1 Long,l2 Long,l3 Long,l4 Long,l5 Long,l6 Long,l7 Long,l8 Long,l9 Long,l10 Long,l11 Long,l12 Long,l13 Long,l14 Long,l15 Long,l16 Long,l17 Long,l18 Long) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST18 VALUES(125,204,103,20,787,528,147,333,400,277,666,1005,2010,1500,888,963,444,777);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF18 AS com.snappy.scala.poc.udf.ScalaUDF18 RETURNS DATE USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF18(l1,l2,l3,l4,l5,l6,l7,l8,l9,l10,l11,l12,l13,l14,l15,l16,l17,l18) FROM ST18;","UDF18");
            assert result.equals(new Date(System.currentTimeMillis()).toString())  : "Result mismatch in Scala UDF18 ->  Expected : " +  new Date(System.currentTimeMillis()).toString() + ", Actual : " + result;
            Log.getLogWriter().info("Scala UDF18 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF18;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST18;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF18");
        }
    }

    private void executeScalaUDF19(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST19;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST19(d1 Double,d2 Double,d3 Double,d4 Double,d5 Double,d6 Double,d7 Double,d8 Double,d9 Double,d10 Double,d11 Double,d12 Double,d13 Double,d14 Double,d15 Double,d16 Double,d17 Double,d18 Double,d19 Double) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST19 VALUES(25.3,200.8,101.5,201.9,789.85,522.398,144.2,336.1,400.0,277.7,666.6,1005.630,2010.96,1500.21,888.7,963.87,416.0,786.687,1.1);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF19 AS com.snappy.scala.poc.udf.ScalaUDF19 RETURNS DOUBLE USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF19(d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19) FROM ST19;","UDF19");
            assert result.equals("11239.505000000001")  : "Result mismatch in Scala UDF19 ->  Expected : 11239.505000000001, Actual : " + result;
            Log.getLogWriter().info("Scala UDF19 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF19;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST19;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF19");
        }
    }

    private void executeScalaUDF20(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST20;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST20(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String,s9 String,s10 String,s11 String,s12 String,s13 String,s14 String,s15 String,s16 String,s17 String,s18 String,s19 String,s20 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST20 VALUES(' Snappy ','   Data   ','  is  ',' the ','  first    ','   product','to ',' integrate   ','a',' database','  into  ','  spark  ','making      ','   Spark   ','  work  ','just    ','like','    a','         database   ',' . ');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF20 AS com.snappy.scala.poc.udf.ScalaUDF20 RETURNS STRING USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF20(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,s15,s16,s17,s18,s19,s20) FROM ST20;","UDF20");
            assert result.equals("Snappy,Data,is,the,first,product,to,integrate,a,database,into,spark,making,Spark,work,just,like,a,database.")  : "Result mismatch in Scala UDF20 ->  Expected : Snappy,Data,is,the,first,product,to,integrate,a,database,into,spark,making,Spark,work,just,like,a,database., Actual : " + result;
            Log.getLogWriter().info("Scala UDF20 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF20;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST20;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF20");
        }
    }

    private void executeScalaUDF21(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST21;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST21(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String,s9 String,s10 String,s11 String,s12 String,s13 String,s14 String,s15 String,s16 String,s17 String,s18 String,s19 String,s20 String,s21 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST21 VALUES('23','21','30','37','25','22','28','32','31','24','24','26','35','89','98','45','54','95','74','66','5');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF21 AS com.snappy.scala.poc.udf.ScalaUDF21 RETURNS FLOAT USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF21(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,s15,s16,s17,s18,s19,s20,s21) FROM ST21;","UDF21");
            assert result.equals("42.095238")  : "Result mismatch in Scala UDF21 ->  Expected : 42.095238, Actual : " + result;
            Log.getLogWriter().info("Scala UDF21 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF21;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST21;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF21");
        }
    }

    private void executeScalaUDF22(Connection jdbcConnection, String jarPath) {
        try {
            String result = "";
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST22;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ST22(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer,i7 Integer,i8 Integer,i9 Integer,i10 Integer,i11 Integer,i12 Integer,i13 Integer,i14 Integer,i15 Integer,i16 Integer,i17 Integer,i18 Integer,i19 Integer,i20 Integer,i21 Integer,i22 Integer) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO ST22 VALUES(2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION ScalaUDF22 AS com.snappy.scala.poc.udf.ScalaUDF22 RETURNS INTEGER USING JAR '" +  jarPath+ "';");
            result =  getUDFResult(jdbcConnection,"SELECT ScalaUDF22(i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15,i16,i17,i18,i19,i20,i21,i22) FROM ST22;","UDF22");
            assert result.equals("4194304")  : "Result mismatch in Scala UDF22 ->  Expected : 4194304, Actual : " + result;
            Log.getLogWriter().info("Scala UDF22 -> " + result);
            jdbcConnection.createStatement().execute("DROP FUNCTION ScalaUDF22;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS ST22;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing ScalaUDF22");
        }
    }
}
