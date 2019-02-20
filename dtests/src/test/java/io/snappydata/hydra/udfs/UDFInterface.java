package io.snappydata.hydra.udfs;

import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.test.util.TestException;

import java.sql.Connection;
import java.sql.SQLException;

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

    private void createJavaUDF(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF1 AS com.snappy.poc.udf.JavaUDF1 RETURNS INTEGER USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF2 AS com.snappy.poc.udf.JavaUDF2 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF3 AS com.snappy.poc.udf.JavaUDF3 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF4 AS com.snappy.poc.udf.JavaUDF4 RETURNS DOUBLE USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF5 AS com.snappy.poc.udf.JavaUDF5 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF6 AS com.snappy.poc.udf.JavaUDF6 RETURNS DECIMAL USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF7 AS com.snappy.poc.udf.JavaUDF7 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF8 AS com.snappy.poc.udf.JavaUDF8 RETURNS SHORT USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF9 AS com.snappy.poc.udf.JavaUDF9 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF10 AS com.snappy.poc.udf.JavaUDF10 RETURNS FLOAT USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF11 AS com.snappy.poc.udf.JavaUDF11 RETURNS BOOLEAN USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF12 AS com.snappy.poc.udf.JavaUDF12 RETURNS DATE USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF13 AS com.snappy.poc.udf.JavaUDF13 RETURNS TIMESTAMP USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF14 AS com.snappy.poc.udf.JavaUDF14 RETURNS LONG USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF15 AS com.snappy.poc.udf.JavaUDF15 RETURNS DOUBLE USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF16 AS com.snappy.poc.udf.JavaUDF16 RETURNS VARCHAR(255) USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF17 AS com.snappy.poc.udf.JavaUDF17 RETURNS BOOLEAN USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF18 AS com.snappy.poc.udf.JavaUDF18 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF19 AS com.snappy.poc.udf.JavaUDF19 RETURNS STRING USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF20 AS com.snappy.poc.udf.JavaUDF20 RETURNS INTEGER USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF21 AS com.snappy.poc.udf.JavaUDF21 RETURNS INTEGER USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
            jdbcConnection.createStatement().execute("CREATE FUNCTION UDF22 AS com.snappy.poc.udf.JavaUDF22 RETURNS INTEGER USING JAR '/home/cbhatt/UDF_Jars_Commadns/udf.jar';");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + ", Error in creating Java UDF functions.");
        }
    }

    private void executeJavaUDF(Connection jdbcConnecton) {
        try {
            jdbcConnecton.createStatement().execute("SELECT UDF1(25);");
            jdbcConnecton.createStatement().execute("SELECT UDF2('Snappy','Data');");
            jdbcConnecton.createStatement().execute("SELECT UDF3(Array('snappydata','udf1testing','successful'),Array('snappydata','udf2testing','successful'),Array('snappydata','udf3testing','successful'));");
            jdbcConnecton.createStatement().execute("SELECT UDF4(Map('Maths',100.0d),Map('Science',96.5d),Map('English',92.0d),Map('Music',98.5d));");
            jdbcConnecton.createStatement().execute("SELECT UDF5(true,true,true,true,true);");
            jdbcConnecton.createStatement().execute("SELECT UDF6(231.45,563.93,899.25,611.98,444.44,999.99);");
            jdbcConnecton.createStatement().execute("SELECT UDF7(10,20,30,40,50,60,Struct(17933,54.68d,'SnappyData',null));");
            jdbcConnecton.createStatement().execute("SELECT UDF8(10L,20L,30L,40L,50L,60L,70L,80L);");
            jdbcConnecton.createStatement().execute("SELECT UDF9('c','h','a','n','d','r','e','s','h');");
            jdbcConnecton.createStatement().execute("SELECT UDF10(12.7f,12.7f,12.7f,12.7f,12.7f,12.7f,12.7f,12.7f,12.7f,12.7f);");
            jdbcConnecton.createStatement().execute("SELECT UDF11(12.1f,12.2f,12.3f,12.4f,12.5f,12.6f,12.7f,12.8f,12.9f,14.1f,12.8f);");
            jdbcConnecton.createStatement().execute("SELECT UDF11(12.1f,12.2f,12.3f,12.4f,12.5f,12.6f,12.7f,12.8f,12.9f,14.1f,15.5f);");
            jdbcConnecton.createStatement().execute("SELECT UDF12('Snappy','Data','Leader','Locator','Server','Cluster','Performance','UI','SQL','DataFrame','Dataset','API');");
            jdbcConnecton.createStatement().execute("SELECT UDF13(10,21,25,54,89,78,63,78,55,13,14,85,71);");
            jdbcConnecton.createStatement().execute("SELECT UDF14('0XFF','10','067','54534','0X897','999','077','1234567891234567','0x8978','015','1005','13177','14736','07777');");
            jdbcConnecton.createStatement().execute("SELECT UDF15(5.23d,99.3d,115.6d,0.9d,78.5d,999.99d,458.32d,133.58d,789.87d,269.21d,333.96d,653.21d,550.32d,489.14d,129.87d);");
            jdbcConnecton.createStatement().execute("SELECT UDF16('snappy','data','is','working','on','a','distributed','data','base.','Its','Performance','is','faster','than','Apache','spark');");
            jdbcConnecton.createStatement().execute("SELECT UDF17(15.2f,17.2f,99.9f,45.2f,71.0f,89.8f,66.6f,23.7f,33.1f,13.5f,77.7f,38.3f,44.4f,21.7f,41.8f,83.2f,78.1f);");
            jdbcConnecton.createStatement().execute("SELECT UDF18('snappy','data','spark','leader','locator','server','memory','eviction','sql','udf','udaf','scala','docker','catalog','smart connector','cores','executor','dir');");
            jdbcConnecton.createStatement().execute("SELECT UDF19('snappy','data','spark','leader','locator','server','memory','eviction','sql','udf','udaf','scala','docker','catalog','smart connector','cores','executor','dir','avro');");
            jdbcConnecton.createStatement().execute("SELECT UDF20(10,20,30,40,50,60,70,80,90,100,13,17,21,25,19,22,86,88,64,58);");
            jdbcConnecton.createStatement().execute("SELECT UDF21('snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy','snappy');");
            jdbcConnecton.createStatement().execute("SELECT UDF22(10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10);");
        }catch(SQLException se) {
            throw new TestException(se.getMessage() + ", Error in executing Java UDF functions.");
        }
    }

    private void dropJavaUDF(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF1;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF2;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF3;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF4;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF5;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF6;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF7;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF8;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF9;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF10;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF11;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF12;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF13;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF14;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF15;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF16;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF17;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF18;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF19;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF20;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF21;");
            jdbcConnection.createStatement().execute("DROP FUNCTION UDF22;");
        } catch(SQLException se) {
            throw new TestException(se.getMessage() + ", Error in dropping Java UDF functions.");
        }
    }

    private void createScalaUDF(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
        } catch (SQLException se) {
            throw new TestException(se.getMessage() + ", Error in creating Scala UDF.");
        }
    }

    private void executeScalaUDF(Connection jdbcConnection) {
        try {
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
            jdbcConnection.createStatement().execute("");
        } catch(SQLException se) {
            throw new TestException(se.getMessage() + ", Error in executing Scala UDF." );
        }
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
        createJavaUDF(jdbcConnection);
        executeJavaUDF(jdbcConnection);
        dropJavaUDF(jdbcConnection);
        closeConnection(jdbcConnection);
    }

    private void create_execute_drop_ScalaUDF(Connection jdbcConnection) {
        createScalaUDF(jdbcConnection);
        executeScalaUDF(jdbcConnection);
        dropScalaUDF(jdbcConnection);
        closeConnection(jdbcConnection);
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
