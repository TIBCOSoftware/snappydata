/**
 * <p>
 * Creation Date : 24th September, 2018
 * CopyRight : SnappyData Inc.
 * Revision History
 * Version 1.0  - 09/24/2018
 * </p>
 */
package io.snappydata.hydra.deployThirdPartyConnectors;

import hydra.Log;
import io.snappydata.hydra.cluster.SnappyTest;
import util.TestException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <b>TestScenario_Deploy_Pkg_Deploy_jar</b> class, test the below commands.
 * <p>
 * <br> 1. deploy package <unique-alias-name> [repos ‘repositories’] [path 'some path to cache resolved jars'].
 * <br> 2. list packages or list jars.
 * <br> 3. undeploy [jar-name].</br>
 *      4. deploy jar <unique-alias-name> 'jars';
 * </p>
 *
 * <p>
 * In this class we are using Maven Central repositories and local machine path as path to resolve the cache.
 * </p>
 * @author <a href="mailto:cbhatt@snappydata.io">Chandresh Bhatt</a>
 * @since 1.0.2
 *
 */
public class TestScenario_Deploy_Pkg_Deploy_jar extends SnappyTest {

    private static TestScenario_Deploy_Pkg_Deploy_jar deploy_Pkg_Jars;
    //TODO Below Maps has hard coded value need to be replaced with Command Line Argument in future.
    private Map<String, String> deployPkg_AliasName_Repository = new LinkedHashMap<>();
    private Map<String, String> deployJar_AliasName_JarName = new LinkedHashMap<>();

    //Below Map stores the Name and Coordinates which is used for deployment.
    static private Map<String, String> source_Alias_Coordinate = new ConcurrentHashMap<>();

    //Below Map stores the result of 'list packages;' or 'list jars;' commands.
    static private Map<String, String> result_Alias_Coordinate = new ConcurrentHashMap<>();

    static short testInvalidCharacterPkg = 0;
    static short testInvalidCharacterJar = 0;
    String userHomeDir = null;


    public TestScenario_Deploy_Pkg_Deploy_jar() {
        if (deployPkg_AliasName_Repository.isEmpty()) {
            deployPkg_AliasName_Repository.put("sparkavro_integration", "com.databricks:spark-avro_2.11:4.0.0");
            deployPkg_AliasName_Repository.put("redshiftdatasource", "com.databricks:spark-redshift_2.10:3.0.0-preview1");
            deployPkg_AliasName_Repository.put("mongodbdatasource", "com.stratio.datasource:spark-mongodb_2.11:0.12.0");
            deployPkg_AliasName_Repository.put("spark_kafka_lowlevelintegration", "com.tresata:spark-kafka_2.10:0.6.0");
        }

        if (deployJar_AliasName_JarName.isEmpty()) {
            deployJar_AliasName_JarName.put("sparkmongodb", "spark-mongodb_2.11-0.12.0.jar");
            deployJar_AliasName_JarName.put("redshift", "spark-redshift-2.0.0.jar");
        }

        if(userHomeDir == null)
            userHomeDir = System.getProperty("user.home");
    }

    /**
     * This method call the deployPackages() method and local cache is used to store the repository locally.
     * It is a Hydra Task.
     *
     * @since 1.0.2
     */
    public static void HydraTask_deployPackageUsingJDBC_LocalCache() {
        deploy_Pkg_Jars = new TestScenario_Deploy_Pkg_Deploy_jar();
        deploy_Pkg_Jars.deployPackages();
    }

    /**
     * This method call the deployJar() method and local cache is used to store the jar path locally.
     * It is a Hydra Task.
     *
     * @since 1.0.2
     */
    public static void HydraTask_deployJarUsingJDBC_LocalCache() {
        deploy_Pkg_Jars = new TestScenario_Deploy_Pkg_Deploy_jar();
        deploy_Pkg_Jars.deployJars();
    }

    /**
     * This method use the bad package name (violate the rule, use characters other than alphabets, number and underscore).
     *
     * @since 1.0.2
     */
    public static void HydraTask_deployPackage_InvalidName() {
        deploy_Pkg_Jars = new TestScenario_Deploy_Pkg_Deploy_jar();
        deploy_Pkg_Jars.deployPackagesWithInvalidName();
        testInvalidCharacterPkg++;
    }

    /**
     * This method use the bad jar name (violate the rule, use characters other than alphabets, number and underscore).
     *
     * @since 1.0.2
     */
    public static void HydraTask_deployJar_InvalidName() {
        deploy_Pkg_Jars = new TestScenario_Deploy_Pkg_Deploy_jar();
        deploy_Pkg_Jars.deployJarWithInvalidName();
        testInvalidCharacterJar++;
    }

    /**
     * This method use the deploy package command with multiple packages (comma delimited packages).
     *
     * @since 1.0.2
     */
    public static void HydraTask_deployCommaDelimitedPkgs_CommaDelimitedJars() {
        deploy_Pkg_Jars = new TestScenario_Deploy_Pkg_Deploy_jar();
        deploy_Pkg_Jars.deployCommaDelimitedPkgs_CommaDelimitedJars();
    }

    /**
     * This method use the deploy package command with multiple packages (space delimited packages).
     *
     * @since 1.0.2
     */
    public static void HydraTask_deploySpaceDelimitedPkgs() {
        deploy_Pkg_Jars = new TestScenario_Deploy_Pkg_Deploy_jar();
        deploy_Pkg_Jars.deploySpaceDelimitedPkgs();
    }

    /**
     * This method use the deploy jar command with multiple packages (space delimited jars).
     *
     * @since 1.0.2
     */
    public static void HydraTask_deploySpaceDelimitedJars() {
        deploy_Pkg_Jars = new TestScenario_Deploy_Pkg_Deploy_jar();
        deploy_Pkg_Jars.deploySpaceDelimitedJars();
    }


    //FIXME :
    //In running cluster when we deployed the packages and jars and if we do not stop the cluster and if we try to deploy the packages again with ivy2 as local cache
    //below exception occurs :
    //snappy-sql> deploy package sparkavro_integration 'com.databricks:spark-avro_2.11:4.0.0' path '/home/cbhatt/TPC';
    //snappy-sql> deploy package sparkavro_integration 'com.databricks:spark-avro_2.11:4.0.0';
    //ERROR 38000: (SQLState=38000 Severity=20000) (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0) The exception 'com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException: myID: 127.0.0.1(8012)<v1>:22584, caused by java.lang.IllegalArgumentException: requirement failed: File com.databricks_spark-avro_2.11-4.0.0.jar was already registered with a different path (old path = /home/cbhatt/TPC/jars/com.databricks_spark-avro_2.11-4.0.0.jar, new path = /home/cbhatt/.ivy2/jars/com.databricks_spark-avro_2.11-4.0.0.jar' was thrown while evaluating an expression.
    //Hence at the starting we need to do the cleanup, stop the cluster and then executes the below test cases.
    //TODO
    //Right now, put the test cases on hold, need to discuss with Neeraj.

    /**
     * This method call the deployPackages() method and ivy2 is used to store the repository locally.
     * It is a Hydra Task.
     *
     * @since 1.0.2
     */
    public static void HydraTask_deployPackageUsingJDBC_ivy2() {
        deploy_Pkg_Jars = new TestScenario_Deploy_Pkg_Deploy_jar();
        deploy_Pkg_Jars.clearAll();
        deploy_Pkg_Jars.deployPackages_ivy2();
    }

    /**
     * This method establish the connection with Snappy SQL via JDBC.
     *
     * @throws SQLException
     */
    private Connection getJDBCConnection() {
        Connection connection = null;
        try {
            connection = getLocatorConnection();
        } catch (SQLException se) {
            throw new TestException("Got exception while establish the connection.....", se);
        }
        return connection;
    }

    /**
     * This method validate the unique-alias-name and Maven co-ordinates provided while deploy the jar.
     *
     * @since 1.0.2
     */
    private void validate_Deployed_Artifacts(Map<String, String> resultMap, Map<String, String> sourceMap) {
        if (resultMap.equals(sourceMap))
            Log.getLogWriter().info("Result alias and coordinates are same as Source alias and coordinates.....");
        else
            Log.getLogWriter().info("Result alias and coordinates are not same as Source alias and coordinates.....");
        //Log.getLogWriter().info("ResultMap");
        //for(Map.Entry<String,String> entry : resultMap.entrySet())
        //{
        //    Log.getLogWriter().info("Name : " + entry.getKey() + ", Coordinate : " + entry.getValue());
        //}

        //Log.getLogWriter().info("SourceMap");
        //for(Map.Entry<String,String> entry : sourceMap.entrySet())
        //{
        //Log.getLogWriter().info("Name : " + entry.getKey() + ", Coordinate : " + entry.getValue());
        //}

    }

    /**
     * This method clear all the instance variables, static variables.
     *
     * @since 1.0.2
     */
    private void clearAll() {
        Log.getLogWriter().info("ClearAll()");
        source_Alias_Coordinate.clear();
        result_Alias_Coordinate.clear();
    }

    /**
     * Establish the connection with Snappy Cluster using JDBC.
     * Fire the deploy package, list package and then undeploy any two packages
     * and again executes the list packages will list the remaining packages.
     * In this method local cache (local repository path) is used.
     *
     * @since 1.0.2
     */
    private void deployPackages() {
        Connection conn = null;
        String deploy_Pkg_Cmd = "";
        final String list_Pkg_Cmd = "list packages;";
        final String path = " path ";
        final String local_repository_path = userHomeDir + "/TPC";
        String undeploy_Pkg_Cmd = "";
        Statement st = null;
        ResultSet rs = null;

        conn = getJDBCConnection();
        try {
            for (Map.Entry<String, String> entry : deployPkg_AliasName_Repository.entrySet()) {
                deploy_Pkg_Cmd = "deploy package " + entry.getKey() + " '" + entry.getValue() + "'" + path + "'" + local_repository_path + "';";
                Log.getLogWriter().info("Executing deploy package command : " + deploy_Pkg_Cmd);
                source_Alias_Coordinate.put(entry.getKey().toLowerCase(), entry.getValue().toLowerCase());
                conn.createStatement().execute(deploy_Pkg_Cmd);
            }

            Log.getLogWriter().info("Executing list packages command : " + list_Pkg_Cmd);
            st = conn.createStatement();
            if (st.execute(list_Pkg_Cmd)) {
                rs = st.getResultSet();
                while (rs.next()) {
                    Log.getLogWriter().info(rs.getString("alias") + "|" + rs.getString("coordinate") + "|" + rs.getString("isPackage"));
                    result_Alias_Coordinate.put(rs.getString("alias").toLowerCase(), rs.getString("coordinate").toLowerCase());
                }
            }

            validate_Deployed_Artifacts(result_Alias_Coordinate, source_Alias_Coordinate);

            st.clearBatch();

            int count = 0;
            for (Map.Entry<String, String> entry : source_Alias_Coordinate.entrySet()) {
                undeploy_Pkg_Cmd = "undeploy " + entry.getKey() + ";";
                Log.getLogWriter().info("Executing undeploy package command : " + undeploy_Pkg_Cmd);
                conn.createStatement().execute(undeploy_Pkg_Cmd);
                source_Alias_Coordinate.remove(entry.getKey());
                result_Alias_Coordinate.remove(entry.getKey());
                count++;
                if (count >= 2)
                    break;
            }

            st.clearBatch();

            Log.getLogWriter().info("Executing list packages command : " + list_Pkg_Cmd);
            st = conn.createStatement();
            if (st.execute(list_Pkg_Cmd)) {
                rs = st.getResultSet();
                Log.getLogWriter().info("Rest of the packages are after undeploy command execution : ");
                while (rs.next()) {
                    Log.getLogWriter().info(rs.getString("alias") + "|" + rs.getString("coordinate") + "|" + rs.getString("isPackage"));
                }
            }
        } catch (SQLException se) {
            throw new TestException("Deploy Package Exception with local repository.....");
        } finally {
            closeConnection(conn);
        }
    }

    /**
     * Establish the connection with Snappy Cluster using JDBC.
     * Fire the deploy jar, list jars and then undeploy any one jar.
     * and again executes the list jars will list the remaining jars.
     * In this method local cache (local repository path) is used.
     *
     * @since 1.0.2
     */
    private void deployJars() {
        Connection conn = null;
        String deploy_Jar_Cmd = "";
        final String list_Jar_Cmd = "list jars;";
        final String local_repository_path = userHomeDir + "/Downloads/Jars/";
        String undeploy_Pkg_Cmd = "";
        Statement st = null;
        ResultSet rs = null;

        conn = getJDBCConnection();
        try {
            for (Map.Entry<String, String> entry : deployJar_AliasName_JarName.entrySet()) {
                deploy_Jar_Cmd = "deploy jar " + entry.getKey() + " '" + local_repository_path + entry.getValue() + "';";
                Log.getLogWriter().info("Executing deploy jar command : " + deploy_Jar_Cmd);
                source_Alias_Coordinate.put(entry.getKey().toLowerCase(), entry.getValue().toLowerCase());
                conn.createStatement().execute(deploy_Jar_Cmd);
            }

            //TODO 'list jars;' command will show the previous packages as well hence result is mismatch while doing validation since source for jar deployment is different.
            Log.getLogWriter().info("Executing list jars command : " + list_Jar_Cmd);
            st = conn.createStatement();
            if (st.execute(list_Jar_Cmd)) {
                rs = st.getResultSet();
                while (rs.next()) {
                    Log.getLogWriter().info(rs.getString("alias") + "|" + rs.getString("coordinate") + "|" + rs.getString("isPackage"));
                    result_Alias_Coordinate.put(rs.getString("alias").toLowerCase(), rs.getString("coordinate").toLowerCase());
                }
            }

            validate_Deployed_Artifacts(result_Alias_Coordinate, source_Alias_Coordinate);

            st.clearBatch();

            int count = 0;
            for (Map.Entry<String, String> entry : source_Alias_Coordinate.entrySet()) {
                undeploy_Pkg_Cmd = "undeploy " + entry.getKey() + ";";
                Log.getLogWriter().info("Executing undeploy package command : " + undeploy_Pkg_Cmd);
                conn.createStatement().execute(undeploy_Pkg_Cmd);
                source_Alias_Coordinate.remove(entry.getKey());
                result_Alias_Coordinate.remove(entry.getKey());
                count++;
                if (count >= 1)
                    break;
            }

            st.clearBatch();

            Log.getLogWriter().info("Executing list packages command : " + list_Jar_Cmd);
            st = conn.createStatement();
            if (st.execute(list_Jar_Cmd)) {
                rs = st.getResultSet();
                Log.getLogWriter().info("Rest of the jars are after undeploy command execution : ");
                while (rs.next()) {
                    Log.getLogWriter().info(rs.getString("alias") + "|" + rs.getString("coordinate") + "|" + rs.getString("isPackage"));
                }
            }
        }
        catch (SQLException se)
        {
            throw new TestException("Deploy Jar Exception with local repository.....");
        }
        finally {
            closeConnection(conn);
        }
    }

    /**
     * Establish the connection with Snappy Cluster using JDBC.
     * Fire the deploy package command and use the special character(characters other than alphabets, numbers and underscore).
     * Method should catch the exception Invalid Name.
     * In this method local cache (local repository path) is used.
     *
     * @since 1.0.2
     */
    private void deployPackagesWithInvalidName()
    {
        Connection conn = null;
        String deploy_Pkg_Cmd = "";
        final String path = " path ";
        final String local_repository_path = userHomeDir + "/TPC";
        String name = null;
        String coordinate = null;

        if(testInvalidCharacterPkg == 0) {
            name = "sparkavro#integration";
            coordinate = "com.databricks:spark-avro_2.11:4.0.0";
        }
        if(testInvalidCharacterPkg == 1) {
            name = "redshift>datasource";
            coordinate = "com.databricks:spark-redshift_2.10:3.0.0-preview1";
        }
        if(testInvalidCharacterPkg == 2) {
            name = "mongodb?datasource";
            coordinate = "com.stratio.datasource:spark-mongodb_2.11:0.12.0";
        }
        if(testInvalidCharacterPkg == 3) {
            name = "spark+kafka_lowlevelintegration";
            coordinate = "com.tresata:spark-kafka_2.10:0.6.0";
            testInvalidCharacterPkg = 0;
        }

        conn = getJDBCConnection();
        try
        {
            deploy_Pkg_Cmd = "deploy package " + name + " '" + coordinate + "'" + path + "'" + local_repository_path + "';";
            Log.getLogWriter().info("Executing deploy package command : " + deploy_Pkg_Cmd);
            conn.createStatement().execute(deploy_Pkg_Cmd);
        }
        catch(SQLException se)
        {
            Log.getLogWriter().info("Invalid Characters encounter in package name other than Alphabets, Numbers or Underscore.....");
        }
         finally
        {
            closeConnection(conn);
        }
    }

    /**
     * Establish the connection with Snappy Cluster using JDBC.
     * Fire the deploy jar command and use the special character(characters other than alphabets, numbers and underscore).
     * Method should catch the exception Invalid Name.
     * In this method local cache (local repository path) is used.
     *
     * @since 1.0.2
     */
    private void deployJarWithInvalidName()
    {
        Connection conn = null;
        String deploy_jar_Cmd = "";
        String name = null;
        String coordinate = null;

        if(testInvalidCharacterJar == 0) {
            name = "SparkMongoDB**";
            coordinate = userHomeDir + "/Downloads/Jars/spark-mongodb_2.11-0.12.0.jar;";
        }
        if(testInvalidCharacterJar == 1) {
            name = "Red$Shift";
            coordinate = userHomeDir + "/Downloads/Jars/spark-redshift-2.0.0.jar;";
            testInvalidCharacterJar = 0;
        }

        conn = getJDBCConnection();
        try
        {
            deploy_jar_Cmd = "deploy jar " + name + " '" + coordinate + "';";
            Log.getLogWriter().info("Executing deploy jar command : " + deploy_jar_Cmd);
            conn.createStatement().execute(deploy_jar_Cmd);
        }
        catch(SQLException se)
        {
            Log.getLogWriter().info("Invalid Characters encounter in jar name other than Alphabets, Numbers or Underscore.....");
        }
        finally
        {
            closeConnection(conn);
        }
    }

    /**
     * Establish the connection with Snappy Cluster using JDBC.
     * Fire the deploy package command with multiple packages.
     * User can provide multiple package with comma delimiter.
     * This method uses the local cache (local repository path).
     *
     * @since 1.0.2
     */
    private void deployCommaDelimitedPkgs_CommaDelimitedJars()
    {
        Connection conn = null;
        String deploy_Pkg_Cmd = "";
        String deploy_Jar_Cmd = "";
        final String list_Pkg_Cmd = "list packages;";
        Statement st = null;
        ResultSet rs = null;

        conn = getJDBCConnection();
        try
        {
            deploy_Pkg_Cmd = "deploy package GraphPkgs 'tdebatty:spark-knn-graphs:0.13,webgeist:spark-centrality:0.11,SherlockYang:spark-cc:0.1' path "  + "'"+ userHomeDir + "/TPC';";
            Log.getLogWriter().info("Executing deploy package command : " + deploy_Pkg_Cmd);
            conn.createStatement().execute(deploy_Pkg_Cmd);

            deploy_Jar_Cmd = "deploy jar SerDe '" + userHomeDir + "/Downloads/Jars/hive-serde-0.8.0.jar," + userHomeDir + "/Downloads/Jars/hive-json-serde.jar';";
            Log.getLogWriter().info("Executing deploy jar command : " + deploy_Jar_Cmd);
            conn.createStatement().execute(deploy_Jar_Cmd);

            Log.getLogWriter().info("Executing list packages command : " + list_Pkg_Cmd);
            st = conn.createStatement();
            if (st.execute(list_Pkg_Cmd)) {
                rs = st.getResultSet();
                while (rs.next()) {
                    Log.getLogWriter().info(rs.getString("alias") + "|" + rs.getString("coordinate") + "|" + rs.getString("isPackage"));
                }
            }

            conn.createStatement().execute("undeploy GraphPkgs;");
            conn.createStatement().execute("undeploy SerDe;");
        }
        catch(SQLException se)
        {
            throw new TestException("Exception in Comma Delimiter Package deployment.....");
        }
        finally
        {
            closeConnection(conn);
        }
    }

    /**
     * Establish the connection with Snappy Cluster using JDBC.
     * Fire the deploy package command with multiple packages.
     * User can provide multiple package with space delimiter. Expected output is method should throw the exception. (Wrong Delimiter used).
     * This method uses the local cache (local repository path).
     */
    private void deploySpaceDelimitedPkgs()
    {
        Connection conn = null;
        String deploy_Pkg_Cmd = "";

        conn = getJDBCConnection();
        try
        {
            deploy_Pkg_Cmd = "deploy package GraphPkgs 'tdebatty:spark-knn-graphs:0.13 webgeist:spark-centrality:0.11,SherlockYang:spark-cc:0.1' path "  + "'"+ userHomeDir + "/TPC';";
            Log.getLogWriter().info("Executing deploy package command : " + deploy_Pkg_Cmd);
            conn.createStatement().execute(deploy_Pkg_Cmd);
        }
        catch(SQLException se)
        {
            Log.getLogWriter().info("deploy package command is not comma delimited.....");
        }
        finally
        {
            closeConnection(conn);
        }
    }

    /**
     * Establish the connection with Snappy Cluster using JDBC.
     * Fire the deploy jar command with multiple packages.
     * User can provide multiple package with space delimiter. Expected output is method should throw the exception. (Wrong Delimiter used).
     * This method uses the local cache (local repository path).
     */
    private void deploySpaceDelimitedJars()
    {
        Connection conn = null;
        String deploy_Jar_Cmd = "";

        conn = getJDBCConnection();
        try
        {
            deploy_Jar_Cmd = "deploy jar SerDe '" + userHomeDir + "/Downloads/Jars/hive-serde-0.8.0.jar " + userHomeDir + "/Downloads/Jars/hive-json-serde.jar';";
            Log.getLogWriter().info("Executing deploy jar command : " + deploy_Jar_Cmd);
            conn.createStatement().execute(deploy_Jar_Cmd);
        }
        catch(SQLException se)
        {
            Log.getLogWriter().info("deploy jar command is not comma delimited.....");
        }
        finally
        {
            closeConnection(conn);
        }
    }





    //FIXME :
    //In running cluster when we deployed the packages and jars and if we do not stop the cluster and if we try to deploy the packages again with ivy2 as local cache
    //below exception occurs :
    //snappy-sql> deploy package sparkavro_integration 'com.databricks:spark-avro_2.11:4.0.0' path '/home/cbhatt/TPC';
    //snappy-sql> deploy package sparkavro_integration 'com.databricks:spark-avro_2.11:4.0.0';
    //ERROR 38000: (SQLState=38000 Severity=20000) (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0) The exception 'com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException: myID: 127.0.0.1(8012)<v1>:22584, caused by java.lang.IllegalArgumentException: requirement failed: File com.databricks_spark-avro_2.11-4.0.0.jar was already registered with a different path (old path = /home/cbhatt/TPC/jars/com.databricks_spark-avro_2.11-4.0.0.jar, new path = /home/cbhatt/.ivy2/jars/com.databricks_spark-avro_2.11-4.0.0.jar' was thrown while evaluating an expression.
    //Hence at the starting we need to do the cleanup, stop the cluster and then executes the below test cases.
    //TODO
    //Right now, put the test cases on hold, need to discuss with Neeraj.

    /**
     * Establish the connection with Snappy Cluster using JDBC.
     * Fire the deploy package, list package and then undeploy any two packages
     * and again executes the list packages will list the remaining packages.
     * In this method local cache (local repository path) is used.
     *
     * @since 1.0.2
     */
    private void deployPackages_ivy2() {
        Connection conn = null;
        String deploy_Pkg_Cmd = "";
        final String list_Pkg_Cmd = "list packages;";
        final String path = " path ";
        final String local_repository_path = "/home/cbhatt/TPC";
        String undeploy_Pkg_Cmd = "";
        Statement st = null;
        ResultSet rs = null;

        conn = getJDBCConnection();
        try {
            for (Map.Entry<String, String> entry : deployPkg_AliasName_Repository.entrySet()) {
                deploy_Pkg_Cmd = "deploy package " + entry.getKey() + " '" + entry.getValue() + "';";
                Log.getLogWriter().info("Executing deploy package command : " + deploy_Pkg_Cmd);
                source_Alias_Coordinate.put(entry.getKey().toLowerCase(), entry.getValue().toLowerCase());
                conn.createStatement().execute(deploy_Pkg_Cmd);
            }

            Log.getLogWriter().info("Executing list packages command : " + list_Pkg_Cmd);
            st = conn.createStatement();
            if (st.execute(list_Pkg_Cmd)) {
                rs = st.getResultSet();
                while (rs.next()) {
                    Log.getLogWriter().info(rs.getString("alias") + "|" + rs.getString("coordinate") + "|" + rs.getString("isPackage"));
                    result_Alias_Coordinate.put(rs.getString("alias").toLowerCase(), rs.getString("coordinate").toLowerCase());
                }
            }

            validate_Deployed_Artifacts(result_Alias_Coordinate, source_Alias_Coordinate);

            st.clearBatch();

            int count = 0;
            for (Map.Entry<String, String> entry : source_Alias_Coordinate.entrySet()) {
                undeploy_Pkg_Cmd = "undeploy " + entry.getKey() + ";";
                Log.getLogWriter().info("Executing undeploy package command : " + undeploy_Pkg_Cmd);
                conn.createStatement().execute(undeploy_Pkg_Cmd);
                source_Alias_Coordinate.remove(entry.getKey());
                result_Alias_Coordinate.remove(entry.getKey());
                count++;
                if (count >= 2)
                    break;
            }

            st.clearBatch();

            Log.getLogWriter().info("Executing list packages command : " + list_Pkg_Cmd);
            st = conn.createStatement();
            if (st.execute(list_Pkg_Cmd)) {
                rs = st.getResultSet();
                Log.getLogWriter().info("Rest of the packages are after undeploy command execution : ");
                while (rs.next()) {
                    Log.getLogWriter().info(rs.getString("alias") + "|" + rs.getString("coordinate") + "|" + rs.getString("isPackage"));
                }
            }
        } catch (SQLException se) {
            throw new TestException("ivy2 - Deploy Package Exception with local repository");
        } finally {
            closeConnection(conn);
        }
    }
}


