
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

    static String createHiveRegions = "create table hive_regions(RegionID int,RegionDescription string) row format delimited fields terminated by ','  ";
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

    static String dropHiveRegions = "drop table if exists default.hive_regions";
    static String dropHiveCategories = "drop table if exists default.hive_categories";
    static String dropHiveShippers = "drop table if exists default.hive_shippers";
    static String dropHiveEmployees = "drop table if exists default.hive_employees";
    static String dropHiveCustomers = "drop table if exists default.hive_customers";
    static String dropHiveOrders = "drop table if exists default.hive_orders";
    static String dropHiveOrderDetails = "drop table if exists default.hive_order_details";
    static String dropHiveProducts = "drop table if exists default.hive_products";
    static String dropHiveSuppliers = "drop table if exists default.hive_suppliers";
    static String dropHiveTerritories = "drop table if exists default.hive_territories";
    static String dropHiveEmployeeTerritories = "drop table if exists default.hive_employee_territories";

    static String createSnappyRegions = "create external table if not exists app.staging_regions using csv options(path 'file:////home/cbhatt/north_wind/regions.csv',header 'true')";
    static String loadSnappyRegions = "create table if not exists app.snappy_regions using column options(BUCKETS '10') as select * from staging_regions";

    static String createSnappyCategories = "create external table if not exists app.staging_categories using csv options(path 'file:////home/cbhatt/north_wind/categories.csv',header 'true')";
    static String loadSnappyCategories = "create table if not exists app.snappy_categories using column options(BUCKETS '10') as select * from staging_categories";

    static String createSnappyShippers = "create external table if not exists app.staging_shippers using csv options(path 'file:////home/cbhatt/north_wind/shippers.csv',header 'true')";
    static String loadSnappyShippers = "create table if not exists app.snappy_shippers using column options(BUCKETS '10') as select * from staging_shippers";

    static String createSnappyEmployees = "create external table if not exists app.staging_employees using csv options(path 'file:////home/cbhatt/north_wind/employees.csv',header 'true')";
    static String loadSnappyEmployees = "create table if not exists app.snappy_employees using column options(BUCKETS '10') as select * from staging_employees";

    static String createSnappyCustomers = "create external table if not exists app.staging_customers using csv options(path 'file:////home/cbhatt/north_wind/customers.csv',header 'true')";
    static String loadSnappyCustomers = "create table if not exists app.snappy_customers using column options(BUCKETS '10') as select * from staging_customers";

    static String createSnappyOrders = "create external table if not exists app.staging_orders using csv options (path 'file:////home/cbhatt/north_wind/orders.csv',header 'true')";
    static String loadSnappyOrders = "create table if not exists app.snappy_orders using column options(BUCKETS '10') as select * from staging_orders";

    static String createSnappyOrderDetails = "create external table if not exists app.staging_order_details using csv options (path 'file:////home/cbhatt/north_wind/order_details.csv',header 'true')";
    static String loadSnappyOrderDetails = "create table if not exists app.snappy_order_details using column options(BUCKETS '10') as select * from staging_order_details";

    static String createSnappyProducts = "create external table if not exists app.staging_products using csv options (path 'file:////home/cbhatt/north_wind/products.csv',header 'true')";
    static String loadSnappyProducts = "create table if not exists app.snappy_products using column options(BUCKETS '10') as select * from staging_products";

    static String createSnappySuppliers = "create external table if not exists app.staging_suppliers using csv options (path 'file:////home/cbhatt/north_wind/suppliers.csv',header 'true')";
    static String loadSnappySuppliers = "create table if not exists app.snappy_suppliers using column options(BUCKETS '10') as select * from staging_suppliers";

    static String createSnappyTerritories = "create external table if not exists app.staging_territories using csv options (path 'file:////home/cbhatt/north_wind/territories.csv',header 'true')";
    static String loadSnappyTerritories = "create table if not exists app.snappy_territories using column options(BUCKETS '10') as select * from staging_territories";

    static String createSnappyEmployeeTerritories = "create external table if not exists app.staging_employee_territories using csv options (path 'file:////home/cbhatt/north_wind/employee_territories.csv',header 'true')";
    static String loadSnappyEmployeeTerritories = "create table if not exists app.snappy_employee_territories using column options(BUCKETS '10') as select * from  staging_employee_territories";


    static String dropStageRegions = "drop table if exists app.staging_regions";
    static String dropSnappyRegions = "drop table if exists app.snappy_regions";

    static String dropStageCategories = "drop table if exists app.staging_categories";
    static String dropSnappyCategories = "drop table if exists app.snappy_categories";

    static String dropStageShippers = "drop table if exists app.staging_shippers";
    static String dropSnappyShippers = "drop table if exists app.snappy_shippers";

    static String dropStageEmployees = "drop table if exists app.staging_employees";
    static String dropSnappyEmployees = "drop table if exists app.snappy_employees";

    static String dropStageCustomers = "drop table if exists app.staging_customers";
    static String dropSnappyCustomers = "drop table if exists app.snappy_customers";

    static String dropStageOrders = "drop table if exists app.staging_orders";
    static String dropSnappyOrders = "drop table if exists app.snappy_orders";

    static String dropStageOrderDetails = "drop table if exists app.staging_order_details";
    static String dropSnappyOrderDetails = "drop table if exists app.snappy_order_details";

    static String dropStageProducts = "drop table if exists app.staging_products";
    static String dropSnappyProducts = "drop table if exists app.snappy_products";

    static String dropStageSuppliers = "drop table if exists app.staging_suppliers";
    static String dropSnappySuppliers = "drop table if exists app.snappy_suppliers";

    static String dropStageTerritories = "drop table if exists app.staging_territories";
    static String dropSnappyTerritories = "drop table if exists app.snappy_territories";

    static String dropStageEmployeeTerritories = "drop table if exists app.staging_employee_territories";
    static String dropSnappyEmployeeTerritories = "drop table if exists app.snappy_employee_territories";

    static String[] snappyQueries = {
            "SELECT FirstName, LastName FROM default.hive_employees ORDER BY LastName",
            "SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal FROM default.hive_orders WHERE Freight >= 500",
            "SELECT ProductID, AVG(UnitPrice) AS AveragePrice FROM default.hive_products GROUP BY ProductID HAVING AVG(UnitPrice) > 70 ORDER BY AveragePrice"
    };
    static String[] beelineQueries = {
            "SELECT FirstName, LastName FROM hive_employees ORDER BY LastName",
            "SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal FROM hive_orders WHERE Freight >= 500",
            "SELECT ProductID, AVG(UnitPrice) AS AveragePrice FROM hive_products GROUP BY ProductID HAVING AVG(UnitPrice) > 70 ORDER BY AveragePrice"
    };

    static String[] joinQueries = {
            "SELECT emp.EmployeeID, emp.FirstName, emp.LastName, o.OrderID, o.OrderDate FROM default.hive_employees emp JOIN snappy_orders o ON (emp.EmployeeID = o.EmployeeID) ORDER BY o.OrderDate",
            //Below Query has different output than Snappy.
            //"SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName FROM default.hive_orders o JOIN default.hive_employees e ON (e.EmployeeID = o.EmployeeID) JOIN snappy_customers c ON (c.CustomerID = o.CustomerID) WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > Cast('1998-01-01' as TIMESTAMP) ORDER BY c.CompanyName"
    };

    static String[] beelineJoinQueries = {
            "SELECT emp.EmployeeID, emp.FirstName, emp.LastName, o.OrderID, o.OrderDate FROM default.hive_employees emp JOIN default.hive_orders o ON (emp.EmployeeID = o.EmployeeID) ORDER BY o.OrderDate"
            //"SELECT emp.EmployeeID, emp.FirstName, emp.LastName, o.OrderID, o.OrderDate FROM snappy_employees emp JOIN snappy_orders o ON (emp.EmployeeID = o.EmployeeID) ORDER BY o.OrderDate",
            //Below Query has different output than Beeline.
            //"SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName FROM snappy_orders o JOIN snappy_employees e ON (e.EmployeeID = o.EmployeeID) JOIN snappy_customers c ON (c.CustomerID = o.CustomerID) WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > Cast('1998-01-01' as TIMESTAMP) ORDER BY c.CompanyName"
    };


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

    public static void HydraTask_DropAllTheTblsIfPresent() {
        try {
            metaStore.snappyConnection.createStatement().execute(setexternalHiveCatalog);
            HydraTask_DropSnappyTables();
            HydraTask_DropBeelineTablesFromSnappy();
        } catch (SQLException se) {
            throw new TestException("Snapppy :  Exception in dropping all tables ", se);
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
            throw new TestException("Beeline : Exception in creating table and loading data",se);
        }
    }

    public static void HydraTask_CreateTableAndLoadDataFromSnappy() {
        try {
            metaStore.snappyConnection.createStatement().execute(createSnappyRegions);
            metaStore.snappyConnection.createStatement().execute(loadSnappyRegions);
            metaStore.snappyConnection.createStatement().execute(createSnappyCategories);
            metaStore.snappyConnection.createStatement().execute(loadSnappyCategories);
            metaStore.snappyConnection.createStatement().execute(createSnappyShippers);
            metaStore.snappyConnection.createStatement().execute(loadSnappyShippers);
            metaStore.snappyConnection.createStatement().execute(createSnappyEmployees);
            metaStore.snappyConnection.createStatement().execute(loadSnappyEmployees);
            metaStore.snappyConnection.createStatement().execute(createSnappyCustomers);
            metaStore.snappyConnection.createStatement().execute(loadSnappyCustomers);
            metaStore.snappyConnection.createStatement().execute(createSnappyOrders);
            metaStore.snappyConnection.createStatement().execute(loadSnappyOrders);
            metaStore.snappyConnection.createStatement().execute(createSnappyOrderDetails);
            metaStore.snappyConnection.createStatement().execute(loadSnappyOrderDetails);
            metaStore.snappyConnection.createStatement().execute(createSnappyProducts);
            metaStore.snappyConnection.createStatement().execute(loadSnappyProducts);
            metaStore.snappyConnection.createStatement().execute(createSnappySuppliers);
            metaStore.snappyConnection.createStatement().execute(loadSnappySuppliers);
            metaStore.snappyConnection.createStatement().execute(createSnappyTerritories);
            metaStore.snappyConnection.createStatement().execute(loadSnappyTerritories);
            metaStore.snappyConnection.createStatement().execute(createSnappyEmployeeTerritories);
            metaStore.snappyConnection.createStatement().execute(loadSnappyEmployeeTerritories);

//            metaStore.snappyRS = metaStore.snappyConnection.createStatement().executeQuery(showTblsApp);
//            while(metaStore.snappyRS.next()) {
//                Log.getLogWriter().info(metaStore.snappyRS.getString(1) + "|" + metaStore.snappyRS.getString(2));
//            }
//
//            metaStore.snappyRS = metaStore.snappyConnection.createStatement().executeQuery("select * from app.snappy_territories");
//            while(metaStore.snappyRS.next()) {
//                Log.getLogWriter().info(metaStore.snappyRS.getString(1) + "," + metaStore.snappyRS.getString(2));
//            }

        } catch (SQLException se) {
            throw new TestException("Snappy : Exception in creating table and loading data");
        }
    }

    public static void HydraTask_QueryBeelineTblsFromSnappy() {
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
            for(int index =0 ; index < snappyQueries.length;index++) {
                metaStore.snappyRS = metaStore.snappyConnection.createStatement().executeQuery(snappyQueries[index]);
                metaStore.rs = metaStore.beelineConnection.createStatement().executeQuery(beelineQueries[index]);
                StructTypeImpl beelineSti = ResultSetHelper.getStructType(metaStore.rs);
                StructTypeImpl snappySti = ResultSetHelper.getStructType(metaStore.snappyRS);
                List<com.gemstone.gemfire.cache.query.Struct> beelineList = ResultSetHelper.asList(metaStore.rs, beelineSti, false);
                List<Struct> snappyList = ResultSetHelper.asList(metaStore.snappyRS, snappySti, false);
                metaStore.snappyRS.close();
                metaStore.rs.close();
                beelineFile = logFile + File.separator + "beelineQuery_" + index + ".out";
                snappyFile = logFile + File.separator + "snappyQuery_" + index + ".out";
                testInstance.listToFile(snappyList, snappyFile);
                testInstance.listToFile(beelineList, beelineFile);
                String msg = testInstance.compareFiles(logFile, beelineFile, snappyFile, false, "query_" + index + "_"+ System.currentTimeMillis());
                snappyList.clear();
                beelineList.clear();
                if (msg.length() > 0) {
                    throw new util.TestException("Validation failed : " + msg);
                }
            }
        } catch(SQLException se) {
            throw new TestException("Snappy : Exception in quering beeline tables", se);
        }
    }

    public static void HydraTask_JoinBetweenHiveAndSnappy() {
        try {
            SnappyDMLOpsUtil testInstance = new SnappyDMLOpsUtil();
            ResultSet verificationRS = null;

            String logFile = getCurrentDirPath() + "/joinresultdir";
            String beelineFile, snappyFile;
            File queryResultDir = new File(logFile);
            if (!queryResultDir.exists())
                queryResultDir.mkdirs();
            for(int index =0 ; index < joinQueries.length;index++) {
                metaStore.snappyRS = metaStore.snappyConnection.createStatement().executeQuery(joinQueries[index]);
//                Log.getLogWriter().info("Joining Result........");
//                while (metaStore.snappyRS.next()) {
//                    Log.getLogWriter().info(metaStore.snappyRS.getString(1) + "," + metaStore.snappyRS.getString(2) + "," + metaStore.snappyRS.getString(3) + "," + metaStore.snappyRS.getString(4) + "," +
//                            metaStore.snappyRS.getString(5));
//                }
                verificationRS = metaStore.snappyConnection.createStatement().executeQuery(beelineJoinQueries[index]);
                StructTypeImpl beelineSti = ResultSetHelper.getStructType(verificationRS);
                StructTypeImpl snappySti = ResultSetHelper.getStructType(metaStore.snappyRS);
                List<com.gemstone.gemfire.cache.query.Struct> beelineList = ResultSetHelper.asList(verificationRS, beelineSti, false);
                List<Struct> snappyList = ResultSetHelper.asList(metaStore.snappyRS, snappySti, false);
                metaStore.snappyRS.close();
                verificationRS.close();
                beelineFile = logFile + File.separator + "beelinejoinQuery_" + index + ".out";
                snappyFile = logFile + File.separator + "snappyjoinQuery_" + index + ".out";
                testInstance.listToFile(snappyList, snappyFile);
                testInstance.listToFile(beelineList, beelineFile);
                String msg = testInstance.compareFiles(logFile, beelineFile, snappyFile, false, "query_" + index);
                snappyList.clear();
                beelineList.clear();
                if (msg.length() > 0) {
                    throw new util.TestException("Validation failed : " + msg);
                }
            }
        } catch(SQLException se) {
            throw new TestException("Snappy : Exception in joing tables", se);
        }
    }

    public static void HydraTask_DropSnappyTables() {
        try {
            metaStore.snappyConnection.createStatement().execute(dropStageRegions);
            metaStore.snappyConnection.createStatement().execute(dropSnappyRegions);
            metaStore.snappyConnection.createStatement().execute(dropStageCategories);
            metaStore.snappyConnection.createStatement().execute(dropSnappyCategories);
            metaStore.snappyConnection.createStatement().execute(dropStageShippers);
            metaStore.snappyConnection.createStatement().execute(dropSnappyShippers);
            metaStore.snappyConnection.createStatement().execute(dropStageEmployees);
            metaStore.snappyConnection.createStatement().execute(dropSnappyEmployees);
            metaStore.snappyConnection.createStatement().execute(dropStageCustomers);
            metaStore.snappyConnection.createStatement().execute(dropSnappyCustomers);
            metaStore.snappyConnection.createStatement().execute(dropStageOrders);
            metaStore.snappyConnection.createStatement().execute(dropSnappyOrders);
            metaStore.snappyConnection.createStatement().execute(dropStageOrderDetails);
            metaStore.snappyConnection.createStatement().execute(dropSnappyOrderDetails);
            metaStore.snappyConnection.createStatement().execute(dropStageProducts);
            metaStore.snappyConnection.createStatement().execute(dropSnappyProducts);
            metaStore.snappyConnection.createStatement().execute(dropStageSuppliers);
            metaStore.snappyConnection.createStatement().execute(dropSnappySuppliers);
            metaStore.snappyConnection.createStatement().execute(dropStageTerritories);
            metaStore.snappyConnection.createStatement().execute(dropSnappyTerritories);
            metaStore.snappyConnection.createStatement().execute(dropStageEmployeeTerritories);
            metaStore.snappyConnection.createStatement().execute(dropSnappyEmployeeTerritories);
        } catch(SQLException se) {
            throw new TestException("Snappy : Exception in dropping staging and Snappy Tables", se);
        }
    }

    public static void HydraTask_DropBeelineTablesFromSnappy() {
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
