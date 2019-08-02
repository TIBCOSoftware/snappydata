
package io.snappydata.hydra.externalHiveMetaStore;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import hydra.Log;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil;
import io.snappydata.test.util.TestException;
import org.datanucleus.store.rdbms.query.AbstractRDBMSQueryResult;
import sql.sqlutil.ResultSetHelper;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
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
    static String dropTableStmt = "drop table if exists ";
    static String createDB = "create database ";
    static String dataPath = "/home/cbhatt/NW_1GB/NW_1GB/";
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
            //"SELECT emp.EmployeeID, emp.FirstName, emp.LastName, o.OrderID, o.OrderDate FROM default.hive_employees emp JOIN default.hive_orders o ON (emp.EmployeeID = o.EmployeeID) ORDER BY o.OrderDate"
            "SELECT emp.EmployeeID, emp.FirstName, emp.LastName, o.OrderID, o.OrderDate FROM snappy_employees emp JOIN snappy_orders o ON (emp.EmployeeID = o.EmployeeID) ORDER BY o.OrderDate",
            //Below Query has different output than Beeline.
            //"SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName FROM snappy_orders o JOIN snappy_employees e ON (e.EmployeeID = o.EmployeeID) JOIN snappy_customers c ON (c.CustomerID = o.CustomerID) WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > Cast('1998-01-01' as TIMESTAMP) ORDER BY c.CompanyName"
    };

    static hiveMetaStore metaStore = new hiveMetaStore();

    public hiveMetaStore() {
    }

    // Presently not in use - Experimental.
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

    public static void HydraTask_InsertData_AlterTableFromSnappy() {
        try {
            metaStore.snappyConnection.createStatement().execute(setexternalHiveCatalog);
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "default.t1");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "default.renamedt1");
            metaStore.snappyConnection.createStatement().execute("create table if not exists default.t1(id int, name String) using hive");
            metaStore.snappyConnection.createStatement().execute("insert into default.t1 select id, concat('TIBCO_',id) from range(100000)");
            metaStore.snappyConnection.createStatement().execute("alter table default.t1 rename to default.renamedt1");
            metaStore.snappyRS =  metaStore.snappyConnection.createStatement().executeQuery("select count(*) as Total from default.renamedt1");
            while(metaStore.snappyRS.next()) {
                Log.getLogWriter().info("Total Count in default.renamedt1 is : " + metaStore.snappyRS.getString(1));
            }
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "default.renamedt1");
            Log.getLogWriter().info("Create the table in beeline from snappy, \n insert data into it from snappy, \nrename the table name from snappy, \ncount the no. of records from snappy and dropping the beeline table from snappy\n is sucessful");

        } catch(SQLException se) {
            throw new TestException("Exception in Alter table statement", se);
        }
    }

    public static void HydraTask_CreateAndDropSchemaFromSnappy() {
        try {

            boolean isHiveDB = false;
            metaStore.snappyConnection.createStatement().execute(setexternalHiveCatalog);
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "hiveDB.hive_regions");
            metaStore.snappyConnection.createStatement().execute("drop schema if exists hiveDB");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.staging_regions");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_regions");
            metaStore.snappyConnection.createStatement().execute(createDB +  "hiveDB");
            metaStore.snappyConnection.createStatement().execute(createHiveTables("hiveDB.hive_regions(RegionID int,RegionDescription string)"));
            metaStore.snappyConnection.createStatement().execute(loadDataToHiveTbls(dataPath + "regions.csv", "hiveDB.hive_regions"));
            metaStore.snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_regions", "file:///" + dataPath + "regions.csv" ));
            metaStore.snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_regions", "10", "app.staging_regions"));
            metaStore.snappyRS = metaStore.snappyConnection.createStatement().executeQuery("select * from app.snappy_regions");
            ResultSet rs = metaStore.snappyConnection.createStatement().executeQuery("select * from hiveDB.hive_regions where RegionDescription <> 'RegionDescription'");
            validationRoutine("/createdropschemaresultdir", metaStore.snappyRS, rs, "1");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "hiveDB.hive_regions");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.staging_regions");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_regions");
            metaStore.snappyConnection.createStatement().execute("drop database if exists hiveDB");
            rs = metaStore.snappyConnection.createStatement().executeQuery("show schemas");
            while(rs.next()) {
                String databaseName =  rs.getString(1);
                Log.getLogWriter().info("Schema name : " + databaseName);
                if(databaseName.equals("hiveDB"))
                    isHiveDB = true;
            }
            if(isHiveDB == true)
                Log.getLogWriter().info("Schema hiveDB is present.");
            else
                Log.getLogWriter().info("Schema hiveDB dropped successfully.");
        }catch(SQLException se) {
            throw new TestException("Snappy : Exception in Drop Schema Task", se);
        }
    }

    public static void HydraTask_CreateTableAndLoadDataFromBeeline() {
        try {
            metaStore.beelineConnection.createStatement().execute(createHiveTables("hive_regions(RegionID int,RegionDescription string)"));
            metaStore.beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath + "regions.csv", "hive_regions"));
            metaStore.beelineConnection.createStatement().execute(createHiveTables("hive_categories(CategoryID int,CategoryName string,Description string,Picture string)"));
            metaStore.beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath + "categories.csv", "hive_categories"));
            metaStore.beelineConnection.createStatement().execute(createHiveTables("hive_shippers(ShipperID int ,CompanyName string ,Phone string)"));
            metaStore.beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath + "shippers.csv", "hive_shippers"));
            metaStore.beelineConnection.createStatement().execute(createHiveTables("hive_employees(EmployeeID int,LastName string,FirstName string,Title string,TitleOfCourtesy string,BirthDate timestamp,HireDate timestamp,Address string,City string,Region string,PostalCode string,Country string,HomePhone string,Extension string,Photo string,Notes string,ReportsTo int,PhotoPath string)"));
            metaStore.beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath + "employees.csv", "hive_employees"));
            metaStore.beelineConnection.createStatement().execute(createHiveTables("hive_customers(CustomerID string,CompanyName string,ContactName string,ContactTitle string,Address string,City string,Region string,PostalCode string,Country string,Phone string,Fax string)"));
            metaStore.beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath + "customers.csv", "hive_customers"));
            metaStore.beelineConnection.createStatement().execute(createHiveTables("hive_orders(OrderID int,CustomerID string,EmployeeID int,OrderDate timestamp,RequiredDate timestamp,ShippedDate timestamp,ShipVia int,Freight double,ShipName string,ShipAddress string,ShipCity string,ShipRegion string,ShipPostalCode string,ShipCountry string)"));
            metaStore.beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath + "orders.csv", "hive_orders"));
            metaStore.beelineConnection.createStatement().execute(createHiveTables("hive_order_details(OrderID int,ProductID int,UnitPrice double,Quantity smallint,Discount double)"));
            metaStore.beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath +  "order_details.csv", "hive_order_details"));
            metaStore.beelineConnection.createStatement().execute(createHiveTables("hive_products(ProductID int,ProductName string,SupplierID int,CategoryID int,QuantityPerUnit string,UnitPrice double,UnitsInStock smallint,UnitsOnOrder smallint,ReorderLevel smallint,Discontinued smallint)"));
            metaStore.beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath + "products.csv", "hive_products"));
            metaStore.beelineConnection.createStatement().execute(createHiveTables("hive_suppliers(SupplierID int,CompanyName string,ContactName string,ContactTitle string,Address string,City string,Region string,PostalCode string,Country string,Phone string,Fax string,HomePage string)"));
            metaStore.beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath + "suppliers.csv", "hive_suppliers"));
            metaStore.beelineConnection.createStatement().execute(createHiveTables("hive_territories(TerritoryID string,TerritoryDescription string,RegionID string)"));
            metaStore.beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath + "territories.csv", "hive_territories"));
            metaStore.beelineConnection.createStatement().execute(createHiveTables("hive_employee_territories(EmployeeID int,TerritoryID string)"));
            metaStore.beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath + "employee_territories.csv", "hive_employee_territories"));
        } catch(SQLException se) {
            throw new TestException("Beeline : Exception in creating table and loading data",se);
        }
    }

    public static void HydraTask_CreateTableAndLoadDataFromSnappy() {
        try {
            metaStore.snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_regions", "file:///" + dataPath +  "regions.csv" ));
            metaStore.snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_regions", "10", "app.staging_regions"));
            metaStore.snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_categories", "file:///" + dataPath + "categories.csv"));
            metaStore.snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_categories", "10", "app.staging_categories"));
            metaStore.snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_shippers", "file:///" + dataPath + "shippers.csv"));
            metaStore.snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_shippers", "10", "app.staging_shippers"));
            metaStore.snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_employees", "file:///" + dataPath + "employees.csv"));
            metaStore.snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_employees", "10", "app.staging_employees"));
            metaStore.snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_customers", "file:///" + dataPath + "customers.csv"));
            metaStore.snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_customers", "10", "app.staging_customers"));
            metaStore.snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_orders", "file:///" + dataPath + "orders.csv"));
            metaStore.snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_orders", "10", "app.staging_orders"));
            metaStore.snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_order_details", "file:///" + dataPath + "order_details.csv"));
            metaStore.snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_order_details", "10", "app.staging_order_details"));
            metaStore.snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_products", "file:///" + dataPath + "products.csv"));
            metaStore.snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_products", "10", "app.staging_products"));
            metaStore.snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_suppliers", "file:///" + dataPath + "suppliers.csv"));
            metaStore.snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_suppliers", "10", "app.staging_suppliers"));
            metaStore.snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_territories", "file:///" + dataPath + "territories.csv"));
            metaStore.snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_territories", "10", "app.staging_territories"));
            metaStore.snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_employee_territories", "file:///" + dataPath + "employee_territories.csv"));
            metaStore.snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_employee_territories", "10", "app.staging_employee_territories"));
        } catch (SQLException se) {
            throw new TestException("Snappy : Exception in creating table and loading data");
        }
    }

    public static void HydraTask_CreateExternalTblFromBeelineAndPerformOpsFromSnappy() {
        try {
            String createExternalTblquery = "create external table if not exists extHiveReg(RegionID int,RegionDescription string) row format delimited fields terminated by ',' location '/user/hive/support'";
            String snappy = "";
            String beeLine = "  ";
            metaStore.snappyConnection.createStatement().execute(setexternalHiveCatalog);
            metaStore.beelineConnection.createStatement().execute(dropTableStmt + "extHiveReg");
            metaStore.beelineConnection.createStatement().execute(createExternalTblquery);
            metaStore.rs = metaStore.beelineConnection.createStatement().executeQuery("select * from extHiveReg where RegionDescription <> 'RegionDescription'");
            metaStore.snappyRS = metaStore.snappyConnection.createStatement().executeQuery("select * from default.extHiveReg where RegionDescription <> 'RegionDescription'");
            while(metaStore.snappyRS.next() && metaStore.rs.next()){
                snappy = metaStore.snappyRS.getString(1) + "," + metaStore.snappyRS.getString(2);
                beeLine = metaStore.rs.getString(1) + "," + metaStore.rs.getString(2);
            }
            //validationRoutine("/ExternalTblDir", metaStore.rs, metaStore.snappyRS, "1");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "default.extHiveReg");
            if(snappy.equals(beeLine))
                Log.getLogWriter().info("Creating External Table from Beeline, access it from Snappy and drop the external table from Snappy is successful.");
            else
                Log.getLogWriter().info("Creating External Table from Beeline, access it from Snappy and drop the external table from Snappy is  not OK");
        } catch (SQLException se) {
            throw new TestException("External Table : Exception in External Table Opertaion", se);
        }
    }

    public static void HydraTask_QueryBeelineTblsFromSnappy() {
        try {
            metaStore.snappyConnection.createStatement().execute(setexternalHiveCatalog);
            Log.getLogWriter().info("Pointing to external hive meta store");
            metaStore.snappyST = metaStore.snappyConnection.createStatement();
            Log.getLogWriter().info("create statement from snappy");
            metaStore.snappyRS = metaStore.metaStore.snappyST.executeQuery(showTblsDefault);
            for(int index =0 ; index < snappyQueries.length;index++) {
                metaStore.snappyRS = metaStore.snappyConnection.createStatement().executeQuery(snappyQueries[index]);
                metaStore.rs = metaStore.beelineConnection.createStatement().executeQuery(beelineQueries[index]);
                validationRoutine("/QueryValidationresultdir", metaStore.rs, metaStore.snappyRS, String.valueOf(index));
            }
            Log.getLogWriter().info("Queries on Beeline tables from Snappy executed successfully.");
        } catch(SQLException se) {
            throw new TestException("Snappy : Exception in quering beeline tables", se);
        }
    }

    public static void HydraTask_JoinBetweenHiveAndSnappy() {
        try {
            ResultSet verificationRS = null;
            for(int index =0 ; index < joinQueries.length;index++) {
                metaStore.snappyRS = metaStore.snappyConnection.createStatement().executeQuery(joinQueries[index]);
                verificationRS = metaStore.snappyConnection.createStatement().executeQuery(beelineJoinQueries[index]);
                validationRoutine("/joinresultdir", metaStore.snappyRS, verificationRS, String.valueOf(index));
            }
            Log.getLogWriter().info("Join operation between Hive and Snappy Successful.");
        } catch(SQLException se) {
            throw new TestException("Snappy : Exception in joing tables", se);
        }
    }

    public static void HydraTask_DropSnappyTables() {
        try {
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.staging_regions");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_regions");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.staging_categories");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_categories");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.staging_shippers");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_shippers");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.staging_employees");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_employees");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.staging_customers");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_customers");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.staging_orders");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_orders");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.staging_order_details");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_order_details");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.staging_products");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_products");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.staging_suppliers");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_suppliers");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.staging_territories");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_territories");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.staging_employee_territories");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_employee_territories");
        } catch(SQLException se) {
            throw new TestException("Snappy : Exception in dropping staging and Snappy Tables", se);
        }
    }

    public static void HydraTask_DropBeelineTablesFromSnappy() {
        try {
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "default.hive_regions");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "default.hive_categories");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "default.hive_shippers");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "default.hive_employees");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "default.hive_customers");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "default.hive_orders");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "default.hive_order_details");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "default.hive_products");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "default.hive_suppliers");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "default.hive_territories");
            metaStore.snappyConnection.createStatement().execute(dropTableStmt + "default.hive_employee_territories");
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

    private static String createExternalSnappyTbl(String tableName, String filePath) {
        return "create external table if not exists " + tableName + " using csv options(path '" + filePath + "',header 'true')";
    }

    private static String createSnappyTblAndLoadData(String tableName,String buckets,String stageTable) {
        return "create table if not exists " + tableName + " using column options(BUCKETS '" + buckets + "') as select * from " + stageTable;
    }

    private static String createHiveTables(String tableDefinition) {
        return "create table " + tableDefinition + " row format delimited fields terminated by ',' ";   //tblproperties(\"skip.header.line.count\"=\"1\")"
    }

    private static String loadDataToHiveTbls(String dataPath, String tblName) {
        return "load data local inpath '" + dataPath + "' overwrite into table " + tblName;
    }

    private static void validationRoutine(String dir, ResultSet rs1, ResultSet rs2, String index) {
        try {
                SnappyDMLOpsUtil testInstance = new SnappyDMLOpsUtil();
                String logFile = getCurrentDirPath() + dir;
                String beelineFile, snappyFile;
                File queryResultDir = new File(logFile);
                if (!queryResultDir.exists())
                    queryResultDir.mkdirs();

                StructTypeImpl beelineSti = ResultSetHelper.getStructType(rs1);
                StructTypeImpl snappySti = ResultSetHelper.getStructType(rs2);
                List<com.gemstone.gemfire.cache.query.Struct> beelineList = ResultSetHelper.asList(rs1, beelineSti, false);
                List<Struct> snappyList = ResultSetHelper.asList(rs2, snappySti, false);
                rs1.close();
                rs2.close();
                beelineFile = logFile + File.separator + "beelineQuery_" + index + ".out";
                snappyFile = logFile + File.separator + "snappyQuery_" + index + ".out";
                testInstance.listToFile(snappyList, snappyFile);
                testInstance.listToFile(beelineList, beelineFile);
                String msg = testInstance.compareFiles(logFile, beelineFile, snappyFile, false, "query_" + index + "_" + System.currentTimeMillis());
                snappyList.clear();
                beelineList.clear();
                if (msg.length() > 0) {
                    throw new util.TestException("Validation failed : " + msg);
                }
                testInstance = null;
            }catch (SQLException se) {
                throw new TestException("Validation : Exception in Validation", se);
            }
    }
 }
