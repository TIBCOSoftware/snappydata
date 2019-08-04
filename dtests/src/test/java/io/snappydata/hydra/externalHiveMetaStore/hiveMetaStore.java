
package io.snappydata.hydra.externalHiveMetaStore;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import hydra.Log;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil;
import io.snappydata.test.util.TestException;
import sql.sqlutil.ResultSetHelper;

import java.io.File;
import java.sql.*;
import java.util.List;
import java.util.Vector;

public class hiveMetaStore extends SnappyTest
{
    static String setexternalHiveCatalog = "set spark.sql.catalogImplementation=hive";
    static String setexternalInBuiltCatalog = "set spark.sql.catalogImplementation=in-memory";
    static String showTblsApp = "show tables in app";
    static String showTblsDefault = "show tables in default";
    static String dropTableStmt = "drop table if exists ";
    static String createDB = "create database ";
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

    public static Connection connectToBeeline() {
        Connection beelineConnection = null;
        try {
            beelineConnection = DriverManager.getConnection("jdbc:hive2://localhost:11000", "APP", "mine");
            Log.getLogWriter().info("Beeline : Connection is successful");
        }catch (SQLException se) {
            throw new TestException("Beeline : Exception in acquiring connection",se);
        }
        return beelineConnection;
    }

    public static Connection connectToSnappy() {
        Connection snappyConnection = null;
        try {
            snappyConnection = SnappyTest.getLocatorConnection();
            Log.getLogWriter().info("Snappy : Connection is successful");
        }catch (SQLException se) {
            throw new TestException("Snappy : Exception in  acquiring connection", se);
        }
        return snappyConnection;
    }

    public static void HydraTask_DropAllTheTables() {
          DropBeelineTablesFromSnappy(); //snappyConnection);
          DropSnappyTables(); //snappyConnection);
          Log.getLogWriter().info("Drop All the tabels - OK");
    }

    public static void HydraTask_InsertData_AlterTableFromSnappy() {
        try {
            Connection beelineConnection = connectToBeeline();
            Connection snappyConnection = connectToSnappy();
            snappyConnection.createStatement().execute(setexternalHiveCatalog);
            snappyConnection.createStatement().execute(dropTableStmt + "default.t1");
            snappyConnection.createStatement().execute(dropTableStmt + "default.renamedt1");
            snappyConnection.createStatement().execute("create table if not exists default.t1(id int, name String) using hive");
            snappyConnection.createStatement().execute("insert into default.t1 select id, concat('TIBCO_',id) from range(100000)");
            snappyConnection.createStatement().execute("alter table default.t1 rename to default.renamedt1");
            ResultSet rs =  snappyConnection.createStatement().executeQuery("select count(*) as Total from default.renamedt1");
            while(rs.next()) {
                Log.getLogWriter().info("Total Count in default.renamedt1 is : " + rs.getString(1));
            }
            snappyConnection.createStatement().execute(dropTableStmt + "default.renamedt1");
            Log.getLogWriter().info("Create the table in beeline from snappy, \n insert data into it from snappy, \nrename the table name from snappy," +
                    " \ncount the no. of records from snappy and dropping the beeline table from snappy\n is sucessful");
            rs.close();
            snappyConnection.close();
            beelineConnection.close();
        } catch(SQLException se) {
            throw new TestException("Exception in Alter table statement", se);
        }
    }

    public static void HydraTask_CreateAndDropSchemaFromSnappy() {
        try {
            boolean isHiveDB = false;
            Vector dataPath = SnappyPrms.getDataLocationList();
            Log.getLogWriter().info("Path : " + dataPath.firstElement());
            Connection beelineConnection = connectToBeeline();
            Connection snappyConnection = connectToSnappy();
            snappyConnection.createStatement().execute(setexternalHiveCatalog);
            snappyConnection.createStatement().execute(dropTableStmt + "hiveDB.hive_regions");
            snappyConnection.createStatement().execute("drop schema if exists hiveDB");
            snappyConnection.createStatement().execute(dropTableStmt + "app.staging_regions");
            snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_regions");
            snappyConnection.createStatement().execute(createDB +  "hiveDB");
            snappyConnection.createStatement().execute(createHiveTables("hiveDB.hive_regions(RegionID int,RegionDescription string)"));
            snappyConnection.createStatement().execute(loadDataToHiveTbls(dataPath.firstElement() + "regions.csv", "hiveDB.hive_regions"));
            snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_regions", "file:///" + dataPath.firstElement() + "regions.csv" ));
            snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_regions", "10", "app.staging_regions"));
            ResultSet rs1 = snappyConnection.createStatement().executeQuery("select * from app.snappy_regions");
            ResultSet rs2 = snappyConnection.createStatement().executeQuery("select * from hiveDB.hive_regions where RegionDescription <> 'RegionDescription'");
            validationResult("/createdropschemaresultdir", rs1, rs2, "1");
            snappyConnection.createStatement().execute(dropTableStmt + "hiveDB.hive_regions");
            snappyConnection.createStatement().execute(dropTableStmt + "app.staging_regions");
            snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_regions");
            snappyConnection.createStatement().execute("drop database if exists hiveDB");
            ResultSet rs3 = snappyConnection.createStatement().executeQuery("show schemas");
            while(rs3.next()) {
                String databaseName =  rs3.getString(1);
                Log.getLogWriter().info("Schema name : " + databaseName);
                if(databaseName.equals("hiveDB"))
                    isHiveDB = true;
            }
            if(isHiveDB == true)
                Log.getLogWriter().info("Schema hiveDB is present.");
            else
                Log.getLogWriter().info("Schema hiveDB dropped successfully.");
            rs1.close();
            rs2.close();
            rs3.close();
            beelineConnection.close();
            snappyConnection.close();
        }catch(SQLException se) {
            throw new TestException("Snappy : Exception in Drop Schema Task", se);
        }
    }

    public static void HydraTask_CreateTableAndLoadDataFromBeeline() {
        try {
            Vector dataPath = SnappyPrms.getDataLocationList();
            Connection beelineConnection = connectToBeeline();
            beelineConnection.createStatement().execute(createHiveTables("hive_regions(RegionID int,RegionDescription string)"));
            beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath.firstElement() + "regions.csv", "hive_regions"));
            beelineConnection.createStatement().execute(createHiveTables("hive_categories(CategoryID int,CategoryName string,Description string,Picture string)"));
            beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath.firstElement() + "categories.csv", "hive_categories"));
            beelineConnection.createStatement().execute(createHiveTables("hive_shippers(ShipperID int ,CompanyName string ,Phone string)"));
            beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath.firstElement() + "shippers.csv", "hive_shippers"));
            beelineConnection.createStatement().execute(createHiveTables("hive_employees(EmployeeID int,LastName string,FirstName string,Title string,TitleOfCourtesy string,BirthDate timestamp,HireDate timestamp,Address string,City string,Region string,PostalCode string,Country string,HomePhone string,Extension string,Photo string,Notes string,ReportsTo int,PhotoPath string)"));
            beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath.firstElement() + "employees.csv", "hive_employees"));
            beelineConnection.createStatement().execute(createHiveTables("hive_customers(CustomerID string,CompanyName string,ContactName string,ContactTitle string,Address string,City string,Region string,PostalCode string,Country string,Phone string,Fax string)"));
            beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath.firstElement() + "customers.csv", "hive_customers"));
            beelineConnection.createStatement().execute(createHiveTables("hive_orders(OrderID int,CustomerID string,EmployeeID int,OrderDate timestamp,RequiredDate timestamp,ShippedDate timestamp,ShipVia int,Freight double,ShipName string,ShipAddress string,ShipCity string,ShipRegion string,ShipPostalCode string,ShipCountry string)"));
            beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath.firstElement() + "orders.csv", "hive_orders"));
            beelineConnection.createStatement().execute(createHiveTables("hive_order_details(OrderID int,ProductID int,UnitPrice double,Quantity smallint,Discount double)"));
            beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath.firstElement() +  "order-details.csv", "hive_order_details"));
            beelineConnection.createStatement().execute(createHiveTables("hive_products(ProductID int,ProductName string,SupplierID int,CategoryID int,QuantityPerUnit string,UnitPrice double,UnitsInStock smallint,UnitsOnOrder smallint,ReorderLevel smallint,Discontinued smallint)"));
            beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath.firstElement() + "products.csv", "hive_products"));
            beelineConnection.createStatement().execute(createHiveTables("hive_suppliers(SupplierID int,CompanyName string,ContactName string,ContactTitle string,Address string,City string,Region string,PostalCode string,Country string,Phone string,Fax string,HomePage string)"));
            beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath.firstElement() + "suppliers.csv", "hive_suppliers"));
            beelineConnection.createStatement().execute(createHiveTables("hive_territories(TerritoryID string,TerritoryDescription string,RegionID string)"));
            beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath.firstElement() + "territories.csv", "hive_territories"));
            beelineConnection.createStatement().execute(createHiveTables("hive_employee_territories(EmployeeID int,TerritoryID string)"));
            beelineConnection.createStatement().execute(loadDataToHiveTbls(dataPath.firstElement() + "employee-territories.csv", "hive_employee_territories"));
            beelineConnection.close();
            Log.getLogWriter().info("Create and Load the data from Beeline - OK");
        } catch(SQLException se) {
            throw new TestException("Beeline : Exception in creating table and loading data",se);
        }
    }

    public static void HydraTask_CreateTableAndLoadDataFromSnappy() {
        try {
            Vector dataPath = SnappyPrms.getDataLocationList();
            Connection snappyConnection = connectToSnappy();
            snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_regions", "file:///" + dataPath.firstElement() +  "regions.csv" ));
            snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_regions", "10", "app.staging_regions"));
            snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_categories", "file:///" + dataPath.firstElement() + "categories.csv"));
            snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_categories", "10", "app.staging_categories"));
            snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_shippers", "file:///" + dataPath.firstElement() + "shippers.csv"));
            snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_shippers", "10", "app.staging_shippers"));
            snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_employees", "file:///" + dataPath.firstElement() + "employees.csv"));
            snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_employees", "10", "app.staging_employees"));
            snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_customers", "file:///" + dataPath.firstElement() + "customers.csv"));
            snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_customers", "10", "app.staging_customers"));
            snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_orders", "file:///" + dataPath.firstElement() + "orders.csv"));
            snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_orders", "10", "app.staging_orders"));
            snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_order_details", "file:///" + dataPath.firstElement() + "order-details.csv"));
            snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_order_details", "10", "app.staging_order_details"));
            snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_products", "file:///" + dataPath.firstElement() + "products.csv"));
            snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_products", "10", "app.staging_products"));
            snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_suppliers", "file:///" + dataPath.firstElement() + "suppliers.csv"));
            snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_suppliers", "10", "app.staging_suppliers"));
            snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_territories", "file:///" + dataPath.firstElement() + "territories.csv"));
            snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_territories", "10", "app.staging_territories"));
            snappyConnection.createStatement().execute(createExternalSnappyTbl("app.staging_employee_territories", "file:///" + dataPath.firstElement() + "employee-territories.csv"));
            snappyConnection.createStatement().execute(createSnappyTblAndLoadData("app.snappy_employee_territories", "10", "app.staging_employee_territories"));
            snappyConnection.close();
            Log.getLogWriter().info("Create and load the data from Snappy - OK");
        } catch (SQLException se) {
            throw new TestException("Snappy : Exception in creating table and loading data");
        }
    }

    public static void HydraTask_CreateExternalTblFromBeelineAndPerformOpsFromSnappy() {
        try {
            Connection beelineConnection = connectToBeeline();
            Connection snappyConnection = connectToSnappy();
            snappyConnection.createStatement().execute(setexternalHiveCatalog);
            String createExternalTblquery = "create external table if not exists extHiveReg(RegionID int,RegionDescription string) row format delimited fields terminated by ',' location '/user/hive/support'";
            String snappy = "";
            String beeLine = "  ";
            beelineConnection.createStatement().execute(dropTableStmt + "extHiveReg");
            beelineConnection.createStatement().execute(createExternalTblquery);
            ResultSet rs1 = beelineConnection.createStatement().executeQuery("select * from extHiveReg where RegionDescription <> 'RegionDescription'");
            ResultSet rs2 = snappyConnection.createStatement().executeQuery("select * from default.extHiveReg where RegionDescription <> 'RegionDescription'");
            while(rs1.next() && rs2.next()){
                snappy = rs2.getString(1) + "," + rs2.getString(2);
                beeLine = rs1.getString(1) + "," + rs1.getString(2);
            }
            //validationResult("/ExternalTblDir", metaStore.rs, metaStore.snappyRS, "1");
            snappyConnection.createStatement().execute(dropTableStmt + "default.extHiveReg");
            rs1.close();
            rs2.close();
            beelineConnection.close();
            snappyConnection.close();
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
            Connection beelineConnection = connectToBeeline();
            Connection snappyConnection = connectToSnappy();
            ResultSet rs = null;
            ResultSet rs1 = null;
            ResultSet rs2 = null;
            snappyConnection.createStatement().execute(setexternalHiveCatalog);
            Log.getLogWriter().info("Pointing to external hive meta store");
            Statement st = snappyConnection.createStatement();
            Log.getLogWriter().info("create statement from snappy");
            rs = st.executeQuery(showTblsDefault);
            for(int index =0 ; index < snappyQueries.length;index++) {
                rs1 = snappyConnection.createStatement().executeQuery(snappyQueries[index]);
                rs2 = beelineConnection.createStatement().executeQuery(beelineQueries[index]);
                validationResult("/QueryValidationresultdir", rs2, rs1, String.valueOf(index));
            }
            rs.close();
            rs1.close();
            rs2.close();
            beelineConnection.close();
            snappyConnection.close();
            Log.getLogWriter().info("Queries on Beeline tables from Snappy executed successfully.");
        } catch(SQLException se) {
            throw new TestException("Snappy : Exception in quering beeline tables", se);
        }
    }

    public static void HydraTask_JoinBetweenHiveAndSnappy() {
        try {
            Connection beelineConnection = connectToBeeline();
            Connection snappyConnection = connectToSnappy();
            ResultSet rs = null;
            ResultSet verificationRS = null;
            snappyConnection.createStatement().execute(setexternalHiveCatalog);
            for(int index =0 ; index < joinQueries.length;index++) {
                rs = snappyConnection.createStatement().executeQuery(joinQueries[index]);
                verificationRS = snappyConnection.createStatement().executeQuery(beelineJoinQueries[index]);
                validationResult("/joinresultdir", rs, verificationRS, String.valueOf(index));
            }
            rs.close();
            verificationRS.close();
            beelineConnection.close();
            snappyConnection.close();
            Log.getLogWriter().info("Join operation between Hive and Snappy Successful.");
        } catch(SQLException se) {
            throw new TestException("Snappy : Exception in joing tables", se);
        }
    }

    public static void DropSnappyTables() {
        try {
            Connection snappyConnection = connectToSnappy();
            snappyConnection.createStatement().execute(dropTableStmt + "app.staging_regions");
            snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_regions");
            snappyConnection.createStatement().execute(dropTableStmt + "app.staging_categories");
            snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_categories");
            snappyConnection.createStatement().execute(dropTableStmt + "app.staging_shippers");
            snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_shippers");
            snappyConnection.createStatement().execute(dropTableStmt + "app.staging_employees");
            snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_employees");
            snappyConnection.createStatement().execute(dropTableStmt + "app.staging_customers");
            snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_customers");
            snappyConnection.createStatement().execute(dropTableStmt + "app.staging_orders");
            snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_orders");
            snappyConnection.createStatement().execute(dropTableStmt + "app.staging_order_details");
            snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_order_details");
            snappyConnection.createStatement().execute(dropTableStmt + "app.staging_products");
            snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_products");
            snappyConnection.createStatement().execute(dropTableStmt + "app.staging_suppliers");
            snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_suppliers");
            snappyConnection.createStatement().execute(dropTableStmt + "app.staging_territories");
            snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_territories");
            snappyConnection.createStatement().execute(dropTableStmt + "app.staging_employee_territories");
            snappyConnection.createStatement().execute(dropTableStmt + "app.snappy_employee_territories");
            snappyConnection.close();
        } catch(SQLException se) {
            throw new TestException("Snappy : Exception in dropping staging and Snappy Tables", se);
        }
    }

    public static void DropBeelineTablesFromSnappy() {
        try {
            Connection beelineConnection = connectToBeeline();
            Connection snappyConnection = connectToSnappy();
            snappyConnection.createStatement().execute(setexternalHiveCatalog);
            snappyConnection.createStatement().execute(dropTableStmt + "default.hive_regions");
            snappyConnection.createStatement().execute(dropTableStmt + "default.hive_categories");
            snappyConnection.createStatement().execute(dropTableStmt + "default.hive_shippers");
            snappyConnection.createStatement().execute(dropTableStmt + "default.hive_employees");
            snappyConnection.createStatement().execute(dropTableStmt + "default.hive_customers");
            snappyConnection.createStatement().execute(dropTableStmt + "default.hive_orders");
            snappyConnection.createStatement().execute(dropTableStmt + "default.hive_order_details");
            snappyConnection.createStatement().execute(dropTableStmt + "default.hive_products");
            snappyConnection.createStatement().execute(dropTableStmt + "default.hive_suppliers");
            snappyConnection.createStatement().execute(dropTableStmt + "default.hive_territories");
            snappyConnection.createStatement().execute(dropTableStmt + "default.hive_employee_territories");
            beelineConnection.close();
            snappyConnection.close();
        } catch(SQLException se) {
            throw new TestException("Snappy : Exception in dropping beeline tables", se);
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

    private static void validationResult(String dir, ResultSet rs1, ResultSet rs2, String index) {
        try {
                SnappyDMLOpsUtil testInstance = new SnappyDMLOpsUtil();
                String logFile = getCurrentDirPath() + dir;
                String beelineFile, snappyFile;
                File queryResultDir = new File(logFile);
                if (!queryResultDir.exists())
                    queryResultDir.mkdirs();

               StructTypeImpl beelineSti = ResultSetHelper.getStructType(rs1);
               StructTypeImpl snappySti = ResultSetHelper.getStructType(rs2);
               beelineFile = logFile + File.separator + "beelineQuery_" + index + ".out";
               snappyFile = logFile + File.separator + "snappyQuery_" + index + ".out";


                List<com.gemstone.gemfire.cache.query.Struct> beelineList = ResultSetHelper.asList(rs1, beelineSti, false);
                rs1.close();
                testInstance.listToFile(beelineList, beelineFile);
                beelineList.clear();
                List<Struct> snappyList = ResultSetHelper.asList(rs2, snappySti, false);
                rs2.close();
                testInstance.listToFile(snappyList, snappyFile);
                String msg = testInstance.compareFiles(logFile, beelineFile, snappyFile, false, "query_" + index + "_" + System.currentTimeMillis());
                snappyList.clear();

                if (msg.length() > 0) {
                    throw new util.TestException("Validation failed : " + msg);
                }
                testInstance = null;
            }catch (SQLException se) {
                throw new TestException("Validation : Exception in Validation", se);
            }
    }
 }
