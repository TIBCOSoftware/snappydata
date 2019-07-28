package io.snappydata.hydra.externalHiveMetaStore;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import hydra.Log;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil;
import io.snappydata.test.util.TestException;
import sql.sqlutil.ResultSetHelper;

import java.io.File;
import java.sql.*;
import java.util.List;

public class hiveMetaStore extends SnappyTest
{
    Connection beelineConnection = null;
    Connection snappyConnection = null;
    ResultSet rs = null;
    Statement st = null;
    ResultSet snappyRS = null;
    Statement snappyST = null;
    ResultSetMetaData rsmd = null;

    static String setexternalHiveCatalog = "set spark.sql.catalogImplementation=hive";
    static String setexternalInBuiltCatalog = "set spark.sql.catalogImplementation=in-memory";
    static String showTblsApp = "show tables in app";
    static String showTblsDefault = "show tables in default";

    static String createHiveRegions = "create table hive_regions(RegionID int,RegionDescription string) row format delimited fields terminated by ',' ";
            //"tblproperties(\"skip.header.line.count\"=\"1\")";
    static String loadHiveRegions = "load data local inpath '/home/cbhatt/north_wind/regions.csv' overwrite into table hive_regions";

    static String createHiveCategories = "create table hive_categories(CategoryID int,CategoryName string,Description string,Picture string) row format delimited fields terminated by ',' ";
            //"tblproperties(\"skip.header.line.count\"=\"1\")";
    static String loadHiveCategories = "load data local inpath '/home/cbhatt/north_wind/categories.csv' overwrite into table hive_categories";

    static String createHiveShippers = "create table hive_shippers(ShipperID int ,CompanyName string ,Phone string) row format delimited fields terminated by ',' ";
            //"tblproperties(\"skip.header.line.count\"=\"1\")";
    static String loadHiveShippers = "load data local inpath '/home/cbhatt/north_wind/shippers.csv' overwrite into table hive_shippers";

    static String createHiveEmployees = "create table hive_employees(EmployeeID int,LastName string,FirstName string,Title string,TitleOfCourtesy string,BirthDate timestamp,HireDate timestamp,Address string,City string,Region string,PostalCode string,Country string,HomePhone string,Extension string,Photo string,Notes string,ReportsTo int,PhotoPath string) row format delimited fields terminated by ',' ";
            //"tblproperties(\"skip.header.line.count\"=\"1\")";
    static String loadHiveEmployees = "load data local inpath '/home/cbhatt/north_wind/employees.csv' overwrite into table hive_employees";

    static String createHiveCustomers = "create table hive_customers(CustomerID string,CompanyName string,ContactName string,ContactTitle string,Address string,City string,Region string,PostalCode string,Country string,Phone string,Fax string) row format delimited fields terminated by ',' ";
            //"tblproperties(\"skip.header.line.count\"=\"1\")";
    static String loadHiveCustomers = "load data local inpath '/home/cbhatt/north_wind/customers.csv' overwrite into table hive_customers";

    static String createHiveOrders = "create table hive_orders(OrderID int,CustomerID string,EmployeeID int,OrderDate timestamp,RequiredDate timestamp,ShippedDate timestamp,ShipVia int,Freight double,ShipName string,ShipAddress string,ShipCity string,ShipRegion string,ShipPostalCode string,ShipCountry string) row format delimited fields terminated by ',' ";
            //"tblproperties(\"skip.header.line.count\"=\"1\")";
    static String loadHiveOrders = "load data local inpath '/home/cbhatt/north_wind/orders.csv' overwrite into table hive_orders";

    static String createHiveOrderDetails = "create table hive_order_details(OrderID int,ProductID int,UnitPrice double,Quantity smallint,Discount double) row format delimited fields terminated by ',' ";
            //"tblproperties(\"skip.header.line.count\"=\"1\")";
    static String loadHiveOrderDetails = "load data local inpath '/home/cbhatt/north_wind/order_details.csv' overwrite into table hive_order_details";

    static String createHiveProducts = "create table hive_products(ProductID int,ProductName string,SupplierID int,CategoryID int,QuantityPerUnit string,UnitPrice double,UnitsInStock smallint,UnitsOnOrder smallint,ReorderLevel smallint,Discontinued smallint) row format delimited fields terminated by ',' ";
            //"tblproperties(\"skip.header.line.count\"=\"1\")";
    static String loadHiveProducts = "load data local inpath '/home/cbhatt/north_wind/products.csv' overwrite into table hive_products";

    static String createHiveSuppliers = "create table hive_suppliers(SupplierID int,CompanyName string,ContactName string,ContactTitle string,Address string,City string,Region string,PostalCode string,Country string,Phone string,Fax string,HomePage string) row format delimited fields terminated by ',' ";
            //"tblproperties(\"skip.header.line.count\"=\"1\")";
    static String loadHiveSuppliers = "load data local inpath '/home/cbhatt/north_wind/suppliers.csv' overwrite into table hive_suppliers";

    static String createHiveTerritories = "create table hive_territories(TerritoryID string,TerritoryDescription string,RegionID string) row format delimited fields terminated by ',' ";
            //"tblproperties(\"skip.header.line.count\"=\"1\")";
    static String loadHiveTerritories = "load data local inpath '/home/cbhatt/north_wind/territories.csv' overwrite into table hive_territories";

    static String createHiveEmployeeTerritories = "create table hive_employee_territories(EmployeeID int,TerritoryID string) row format delimited fields terminated by ',' ";
            //"tblproperties(\"skip.header.line.count\"=\"1\")";
    static String loadHiveEmployeeTerritories = "load data local inpath '/home/cbhatt/north_wind/employee_territories.csv' overwrite into table hive_employee_territories";

    static String dropHiveRegions = "drop table default.hive_regions";
    static String dropHiveCategories = "drop table default.hive_categories";
    static String dropHiveShippers = "drop table default.hive_shippers";
    static String dropHiveEmployees = "drop table default.hive_employees";
    static String dropHiveCustomers = "drop table default.hive_customers";
    static String dropHiveOrders = "drop table default.hive_orders";
    static String dropHiveOrderDetails = "drop table default.hive_order_details";
    static String dropHiveProducts = "drop table default.hive_products";
    static String dropHiveSuppliers = "drop table default.hive_suppliers";
    static String dropHiveTerritories = "drop table default.hive_territories";
    static String dropHiveEmployeeTerritories = "drop table default.hive_employee_territories";

    static hiveMetaStore metaStore = new hiveMetaStore();


    public hiveMetaStore() {
    }

    // Presently not in use.
    public static void HydraTask_Wait() {
        try {
            Log.getLogWriter().info("Waiting for 30 seconds");
            Thread.sleep(30000);
        } catch(InterruptedException e) {
            Log.getLogWriter().info(e.getMessage());
        }
    }

    public static void HydraTask_ConnectToBeeline() {
        try {
            metaStore.beelineConnection = DriverManager.getConnection("jdbc:hive2://localhost:11000", "APP", "mine");
            Log.getLogWriter().info("Beeline : Connection is successful");
        }catch (SQLException se) {
            throw new TestException("Beeline : Exception in acquiring connection",se);
        }
    }

    public static void HydraTask_ConnectToSnappy() {
        try {
            metaStore.snappyConnection = SnappyTest.getLocatorConnection();
            Log.getLogWriter().info("Snappy : Connection is successful");
        }catch (SQLException se) {
            throw new TestException("Snappy : Exception in  acquiring connection", se);
        }
    }

    public static void HydraTask_CreateTableAndLoadDataFromBeeline() {
        try {
            metaStore.beelineConnection.createStatement().execute(createHiveRegions);
            metaStore.beelineConnection.createStatement().execute(loadHiveRegions);

            metaStore.beelineConnection.createStatement().execute(createHiveCategories);
            metaStore.beelineConnection.createStatement().execute(loadHiveCategories);

            metaStore.beelineConnection.createStatement().execute(createHiveShippers);
            metaStore.beelineConnection.createStatement().execute(loadHiveShippers);

            metaStore.beelineConnection.createStatement().execute(createHiveEmployees);
            metaStore.beelineConnection.createStatement().execute(loadHiveEmployees);

            metaStore.beelineConnection.createStatement().execute(createHiveCustomers);
            metaStore.beelineConnection.createStatement().execute(loadHiveCustomers);

            metaStore.beelineConnection.createStatement().execute(createHiveOrders);
            metaStore.beelineConnection.createStatement().execute(loadHiveOrders);

            metaStore.beelineConnection.createStatement().execute(createHiveOrderDetails);
            metaStore.beelineConnection.createStatement().execute(loadHiveOrderDetails);

            metaStore.beelineConnection.createStatement().execute(createHiveProducts);
            metaStore.beelineConnection.createStatement().execute(loadHiveProducts);

            metaStore.beelineConnection.createStatement().execute(createHiveSuppliers);
            metaStore.beelineConnection.createStatement().execute(loadHiveSuppliers);

            metaStore.beelineConnection.createStatement().execute(createHiveTerritories);
            metaStore.beelineConnection.createStatement().execute(loadHiveTerritories);

            metaStore.beelineConnection.createStatement().execute(createHiveEmployeeTerritories);
            metaStore.beelineConnection.createStatement().execute(loadHiveEmployeeTerritories);
        } catch(SQLException se) {
            throw new TestException("Beeline : Exception in creating and loading data",se);
        }
    }

    public static void HydraTask_RunQueriesFromSnappy() {
        try {

            SnappyDMLOpsUtil testInstance = new SnappyDMLOpsUtil();

            String logFile = getCurrentDirPath() + "/resultdir";
            String beelineFile, snappyFile;

            File queryResultDir = new File(logFile);
            if (!queryResultDir.exists())
                queryResultDir.mkdirs();


            metaStore.snappyConnection.createStatement().execute(setexternalHiveCatalog);
            Log.getLogWriter().info("Pointing to external hive meta store");
            metaStore.snappyST = metaStore.snappyConnection.createStatement();
            Log.getLogWriter().info("create statement from snappy");
            metaStore.snappyRS = metaStore.metaStore.snappyST.executeQuery(showTblsDefault);
            Log.getLogWriter().info("execute show tables in default");
            Log.getLogWriter().info("---------------------------------------------------------------------------------------------------------------");
            metaStore.snappyRS = metaStore.snappyConnection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery("SELECT FirstName, LastName FROM default.hive_employees ORDER BY LastName");
            metaStore.rs = metaStore.beelineConnection.createStatement().executeQuery("SELECT FirstName, LastName FROM hive_employees ORDER BY LastName");

//            metaStore.snappyRS.first();
//            metaStore.snappyRS.next();

            StructTypeImpl beelineSti = ResultSetHelper.getStructType(metaStore.snappyRS);
            StructTypeImpl snappySti = ResultSetHelper.getStructType(metaStore.rs);
            List<com.gemstone.gemfire.cache.query.Struct> beelineList = ResultSetHelper.asList(metaStore.rs, beelineSti, false);
            List<Struct> snappyList = ResultSetHelper.asList(metaStore.snappyRS, snappySti, false);
            metaStore.snappyRS.close();
            metaStore.rs.close();
            beelineFile = logFile + File.separator + "beelineQuery_" + "Q1" +  ".out";
            snappyFile = logFile + File.separator + "snappyQuery_" + "Q1" +  ".out";

            testInstance.listToFile(snappyList, snappyFile);
            testInstance.listToFile(beelineList,beelineFile);

            String msg = testInstance.compareFiles(logFile, beelineFile, snappyFile, false, "query_"  + "Q1");
            if(msg.length() > 0 ) {
                throw new util.TestException("Validation failed : " + msg);
            }
        } catch(SQLException se) {
            throw new TestException("Snappy : Exception in quering beeline tables", se);
        }
    }

    public static void HydraTask_DropBeelineTables() {
        try {
            metaStore.snappyConnection.createStatement().execute(dropHiveRegions);
            metaStore.snappyConnection.createStatement().execute(dropHiveCategories);
            metaStore.snappyConnection.createStatement().execute(dropHiveShippers);
            metaStore.snappyConnection.createStatement().execute(dropHiveEmployees);
            metaStore.snappyConnection.createStatement().execute(dropHiveCustomers);
            metaStore.snappyConnection.createStatement().execute(dropHiveOrders);
            metaStore.snappyConnection.createStatement().execute(dropHiveOrderDetails);
            metaStore.snappyConnection.createStatement().execute(dropHiveProducts);
            metaStore.snappyConnection.createStatement().execute(dropHiveSuppliers);
            metaStore.snappyConnection.createStatement().execute(dropHiveTerritories);
            metaStore.snappyConnection.createStatement().execute(dropHiveEmployeeTerritories);
        } catch(SQLException se) {
            throw new TestException("Snappy : Exception in dropping beeline tables", se);
        }
    }

    public static  void HydraTask_DisconnectBeeline(){
        try {
            metaStore.beelineConnection.close();
            if(metaStore.beelineConnection.isClosed()) {
                Log.getLogWriter().info("Beeline : Connection closed");
            }
        } catch(SQLException se) {
            throw new TestException("Beeline : Exception in closing connection",se);
        }
    }

    public static  void HydraTask_DisconnectSnappy(){
        try {
            metaStore.snappyConnection.close();
            if(metaStore.snappyConnection.isClosed()) {
                Log.getLogWriter().info("Snappy : Connection closed");
            }
        } catch(SQLException se) {
            throw new TestException("Snappy : Exception in closing connection",se);
        }
    }

    // Presently not in use - Experimental.
    public static void  HydraTask_TestBeeline() {
        try {
            metaStore.st = metaStore.beelineConnection.createStatement();
            metaStore.rs = metaStore.st.executeQuery(showTblsDefault);
            while(metaStore.rs.next()) {
                Log.getLogWriter().info("Beeline Result : " + metaStore.rs.getString(1));
            }
        }catch(SQLException se) {
            throw new TestException("Beeline : Exception for show tables", se);
        }
    }

    // Presently not in use  - Experimental.
    public static void  HydraTask_TestSnappy() {
        try {
            metaStore.snappyConnection.createStatement().execute(setexternalHiveCatalog);
            Log.getLogWriter().info("Pointing to external hive meta store");
            metaStore.snappyST = metaStore.snappyConnection.createStatement();
            Log.getLogWriter().info("create statement from snappy");
            metaStore.snappyRS = metaStore.metaStore.snappyST.executeQuery(showTblsDefault);
            Log.getLogWriter().info("execute show tables in default");
            while(metaStore.snappyRS.next()) {
                Log.getLogWriter().info("Snappy Result : " + metaStore.snappyRS.getString(1) + " | " + metaStore.snappyRS.getString(2 ));
            }
        }catch(SQLException se) {
            throw new TestException("Snappy : Exception for show tables", se);
        }
    }
}
