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

    private void executeJavaUDF1(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T1;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T1(id Integer) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T1 VALUES(25);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF1 AS com.snappy.poc.udf.JavaUDF1 RETURNS INTEGER USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF1(id) FROM T1;","UDF1");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF1;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T1;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF1");
        }
    }

    private void executeJavaUDF2(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T2;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T2(s1 String, s2 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T2 VALUES('Snappy','Data');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF2 AS com.snappy.poc.udf.JavaUDF2 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF2(s1,s2) FROM T2;","UDF2");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF2;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T2;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF2");
        }
    }

    private void executeJavaUDF3(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T3;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T3(arr1 Array<String>,arr2 Array<String>,arr3 Array<String>) USING COLUMN;");
            //jdbcConnection.createStatement().execute("INSERT INTO T3 VALUES(array('snappydata','udf1testing','successful'),array('snappydata','udf2testing','successful'),array('snappydata','udf3testing','successful'));");
            jdbcConnection.createStatement().execute("INSERT INTO T3 SELECT Array('snappydata','udf1testing','successful'),Array('snappydata','udf2testing','successful'),Array('snappydata','udf3testing','successful');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF3 AS com.snappy.poc.udf.JavaUDF3 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF3(arr1,arr2,arr3) FROM T3;","UDF3");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF3;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T3;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF3");
        }
    }

    private void executeJavaUDF4(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T4;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T4(m1 Map<String,Double>,m2 Map<String,Double>,m3 Map<String,Double>,m4 Map<String,Double>) USING COLUMN");
            jdbcConnection.createStatement().execute("INSERT INTO T4 SELECT Map('Maths',100.0d),Map('Science',96.5d),Map('English',92.0d),Map('Music',98.5d);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF4 AS com.snappy.poc.udf.JavaUDF4 RETURNS DOUBLE USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF4(m1,m2,m3,m4) FROM T4;","UDF4");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF4;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T4;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF4");
        }
    }

    private void executeJavaUDF5(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T5;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T5(b1 Boolean,b2 Boolean,b3 Boolean,b4 Boolean,b5 Boolean) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T5 VALUES(true,true,true,true,true);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF5 AS com.snappy.poc.udf.JavaUDF5 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF5(b1,b2,b3,b4,b5) FROM T5;","UDF5");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF5;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T5;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF5");
        }
    }

    private void executeJavaUDF6(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T6;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T6(d1 Decimal,d2 Decimal,d3 Decimal,d4 Decimal,d5 Decimal,d6 Decimal) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T6 VALUES(231.45,563.93,899.25,611.98,444.44,999.99);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF6 AS com.snappy.poc.udf.JavaUDF6 RETURNS DECIMAL USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF6(d1,d2,d3,d4,d5,d6) FROM T6;","UDF6");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF6;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T6;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF6");
        }
    }

    private void executeJavaUDF7(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T7;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T7(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer,st Struct<ist:Integer,dst:Double,sst:String,snull:Integer>) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T7 SELECT 10,20,30,40,50,60,Struct(17933,54.68d,'SnappyData','');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF7 AS com.snappy.poc.udf.JavaUDF7 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF7(i1,i2,i3,i4,i5,i6,st) FROM T7;","UDF7");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF7;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T7;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF7");
        }
    }

    private void executeJavaUDF8(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T8;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T8(l1 Long,l2 Long,l3 Long,l4 Long,l5 Long,l6 Long,l7 Long,l8 Long) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T8 VALUES(10,20,30,40,50,60,70,80);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF8 AS com.snappy.poc.udf.JavaUDF8 RETURNS SHORT USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF8(l1,l2,l3,l4,l5,l6,l7,l8) FROM T8;","UDF8");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF8;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T8;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF8");
        }
    }

    private void executeJavaUDF9(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T9;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T9(c1 Varchar(9),c2 Varchar(9),c3 Varchar(9),c4 Varchar(9),c5 Varchar(9),c6 Varchar(9),c7 Varchar(9),c8 Varchar(9),c9 Varchar(9)) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T9 VALUES('c','h','a','n','d','r','e','s','h');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF9 AS com.snappy.poc.udf.JavaUDF9 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF9(c1,c2,c3,c4,c5,c6,c7,c8,c9) FROM T9;","UDF9");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF9;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T9;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF9");
        }
    }

    private void executeJavaUDF10(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T10;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T10(f1 Float,f2 Float,f3 Float,f4 Float,f5 Float,f6 Float,f7 Float,f8 Float,f9 Float,f10 Float) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T10 VALUES(12.7,12.7,12.7,12.7,12.7,12.7,12.7,12.7,12.7,12.7);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF10 AS com.snappy.poc.udf.JavaUDF10 RETURNS FLOAT USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF10(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10) FROM T10;","UDF10");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF10;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T10;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF10");
        }
    }

    private void executeJavaUDF11(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T11;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T11(f1 Float,f2 Float,f3 Float,f4 Float,f5 Float,f6 Float,f7 Float,f8 Float,f9 Float,f10 Float,f11 Float) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T11 VALUES(12.1,12.2,12.3,12.4,12.5,12.6,12.7,12.8,12.9,14.1,12.8);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF11 AS com.snappy.poc.udf.JavaUDF11 RETURNS BOOLEAN USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF11(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11) FROM T11;","UDF11");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF11;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T11;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF11");
        }
    }

    private void executeJavaUDF11_1(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T11;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T11(f1 Float,f2 Float,f3 Float,f4 Float,f5 Float,f6 Float,f7 Float,f8 Float,f9 Float,f10 Float,f11 Float) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T11 VALUES(12.1,12.2,12.3,12.4,12.5,12.6,12.7,12.8,12.9,14.1,15.5);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF11 AS com.snappy.poc.udf.JavaUDF11 RETURNS BOOLEAN USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF11(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11) FROM T11;","UDF11");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF11;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T11;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF11");
        }
    }

    private void executeJavaUDF12(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T12;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T12(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String,s9 String,s10 String,s11 String,s12 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T12 VALUES('Snappy','Data','Leader','Locator','Server','Cluster','Performance','UI','SQL','DataFrame','Dataset','API');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF12 AS com.snappy.poc.udf.JavaUDF12 RETURNS DATE USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF12(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12) FROM T12;","UDF12");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF12;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T12;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF12");
        }
    }

    private void executeJavaUDF13(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T13;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T13(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer,i7 Integer,i8 Integer,i9 Integer,i10 Integer,i11 Integer,i12 Integer,i13 Integer) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T13 VALUES(10,21,25,54,89,78,63,78,55,13,14,85,71);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF13 AS com.snappy.poc.udf.JavaUDF13 RETURNS TIMESTAMP USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF13(i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13) FROM T13;","UDF13");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF13;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T13;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF13");
        }
    }

    private void executeJavaUDF14(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T14;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T14(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String,s9 String,s10 String,s11 String,s12 String,s13 String,s14 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T14 VALUES('0XFF','10','067','54534','0X897','999','077','1234567891234567','0x8978','015','1005','13177','14736','07777');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF14 AS com.snappy.poc.udf.JavaUDF14 RETURNS LONG USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF14(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14) FROM T14;","UDF14");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF14;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T14;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF14");
        }
    }

    private void executeJavaUDF15(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T15;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T15(d1 Double,d2 Double,d3 Double,d4 Double,d5 Double,d6 Double,d7 Double,d8 Double,d9 Double,d10 Double,d11 Double,d12 Double,d13 Double,d14 Double,d15 Double) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T15 VALUES(5.23,99.3,115.6,0.9,78.5,999.99,458.32,133.58,789.87,269.21,333.96,653.21,550.32,489.14,129.87);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF15 AS com.snappy.poc.udf.JavaUDF15 RETURNS DOUBLE USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF15(d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15) FROM T15;","UDF15");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF15;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T15;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF15");
        }
    }

    private void executeJavaUDF16(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T16;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T16(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String,s9 String,s10 String,s11 String,s12 String,s13 String,s14 String,s15 String,s16 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T16 VALUES('snappy','data','is','working','on','a','distributed','data','base.','Its','Performance','is','faster','than','Apache','spark');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF16 AS com.snappy.poc.udf.JavaUDF16 RETURNS VARCHAR(255) USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF16(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,s15,s16) FROM T16;","UDF16");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF16;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T16;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF16");
        }
    }

    private void executeJavaUDF17(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T17;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T17(f1 Float,f2 Float,f3 Float,f4 Float,f5 Float,f6 Float,f7 Float,f8 Float,f9 Float,f10 Float,f11 Float,f12 Float,f13 Float,f14 Float,f15 Float,f16 Float,f17 Float) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T17 VALUES(15.2,17.2,99.9,45.2,71.0,89.8,66.6,23.7,33.1,13.5,77.7,38.3,44.4,21.7,41.8,83.2,78.1);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF17 AS com.snappy.poc.udf.JavaUDF17 RETURNS BOOLEAN USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF17(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17) FROM T17;","UDF17");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF17;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T17;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF17");
        }
    }

    private void executeJavaUDF18(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T18;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T18(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String,s9 String,s10 String,s11 String,s12 String,s13 String,s14 String,s15 String,s16 String,s17 String,s18 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T18 VALUES('snappy','data','spark','leader','locator','server','memory','eviction','sql','udf','udaf','scala','docker','catalog','smart connector','cores','executor','dir');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF18 AS com.snappy.poc.udf.JavaUDF18 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF18(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,s15,s16,s17,s18) FROM T18;","UDF18");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF18;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T18;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF18");
        }
    }

    private void executeJavaUDF19(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T19;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T19(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String,s9 String,s10 String,s11 String,s12 String,s13 String,s14 String,s15 String,s16 String,s17 String,s18 String,s19 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T19 VALUES('snappy','data','spark','leader','locator','server','memory','eviction','sql','udf','udaf','scala','docker','catalog','smart connector','cores','executor','dir','avro');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF19 AS com.snappy.poc.udf.JavaUDF19 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF19(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,s15,s16,s17,s18,s19) FROM T19;","UDF19");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF19;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T19;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF19");
        }
    }

    private void executeJavaUDF20(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T20;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T20(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer,i7 Integer,i8 Integer,i9 Integer,i10 Integer,i11 Integer,i12 Integer,i13 Integer,i14 Integer,i15 Integer,i16 Integer,i17 Integer,i18 Integer,i19 Integer,i20 Integer) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T20 VALUES(10,20,30,40,50,60,70,80,90,100,13,17,21,25,19,22,86,88,64,58);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF20 AS com.snappy.poc.udf.JavaUDF20 RETURNS INTEGER USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF20(i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15,i16,i17,i18,i19,i20) FROM T20;","UDF20");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF20;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T20;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF20");
        }
    }

    private void executeJavaUDF21(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T21;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T21(s1 String,s2 String,s3 String,s4 String,s5 String,s6 String,s7 String,s8 String,s9 String,s10 String,s11 String,s12 String,s13 String,s14 String,s15 String,s16 String,s17 String,s18 String,s19 String,s20 String,s21 String) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T21 VALUES('snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy');");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF21 AS com.snappy.poc.udf.JavaUDF21 RETURNS INTEGER USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF21(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,s15,s16,s17,s18,s19,s20,s21) FROM T21;","UDF21");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF21;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T21;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF21");
        }
    }

    private void executeJavaUDF22(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T22;");
            jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS T22(i1 Integer,i2 Integer,i3 Integer,i4 Integer,i5 Integer,i6 Integer,i7 Integer,i8 Integer,i9 Integer,i10 Integer,i11 Integer,i12 Integer,i13 Integer,i14 Integer,i15 Integer,i16 Integer,i17 Integer,i18 Integer,i19 Integer,i20 Integer,i21 Integer,i22 Integer) USING COLUMN;");
            jdbcConnection.createStatement().execute("INSERT INTO T22 VALUES(10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10);");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF22 AS com.snappy.poc.udf.JavaUDF22 RETURNS INTEGER USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            getUDFResult(jdbcConnection,"SELECT UDF22(i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15,i16,i17,i18,i19,i20,i21,i22) FROM T22;","UDF22");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF22;");
            jdbcConnection.createStatement().execute("DROP TABLE IF EXISTS T22;");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + " Error in executing Java UDF22");
        }
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
        executeJavaUDF1(jdbcConnection);
        executeJavaUDF2(jdbcConnection);
        executeJavaUDF3(jdbcConnection);
        executeJavaUDF4(jdbcConnection);
        executeJavaUDF5(jdbcConnection);
        executeJavaUDF6(jdbcConnection);
        executeJavaUDF7(jdbcConnection);
        executeJavaUDF8(jdbcConnection);
        executeJavaUDF9(jdbcConnection);
        executeJavaUDF10(jdbcConnection);
        executeJavaUDF11(jdbcConnection);
        executeJavaUDF11_1(jdbcConnection);
        executeJavaUDF12(jdbcConnection);
        executeJavaUDF13(jdbcConnection);
        executeJavaUDF14(jdbcConnection);
        executeJavaUDF15(jdbcConnection);
        executeJavaUDF16(jdbcConnection);
        executeJavaUDF17(jdbcConnection);
        executeJavaUDF18(jdbcConnection);
        executeJavaUDF19(jdbcConnection);
        executeJavaUDF20(jdbcConnection);
        executeJavaUDF21(jdbcConnection);
        executeJavaUDF22(jdbcConnection);
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
