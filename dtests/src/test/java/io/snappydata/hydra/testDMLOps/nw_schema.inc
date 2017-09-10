io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createSchemas = "create schema app";

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createTablesStatements =
"create table app.regions (RegionID int,RegionDescription varchar(100),tid int)"
"create table app.categories (CategoryID int,CategoryName varchar(100),Description varchar(100),Picture varchar(300),tid int)"
"create table app.shippers (ShipperID int not null,CompanyName varchar(100) not null,Phone varchar(100),tid int)"
"create table app.employees(EmployeeID int,LastName varchar(100),FirstName varchar(100),Title varchar(100),TitleOfCourtesy varchar(100),BirthDate timestamp,HireDate timestamp,Address varchar(100),City varchar(100),Region varchar(100),PostalCode varchar(100),Country varchar(100), HomePhone varchar(100),Extension varchar(100),Photo varchar(300),Notes varchar(300),ReportsTo int,PhotoPath varchar(100),tid int)"
"create table app.customers(CustomerID varchar(100),CompanyName varchar(100),ContactName varchar(100),ContactTitle varchar(100),Address varchar(100),City varchar(100),Region varchar(100),PostalCode varchar(100),Country varchar(100),Phone varchar(100),Fax varchar(100),tid int)"
"create table app.orders (OrderID int,CustomerID varchar(20),EmployeeID int,OrderDate timestamp,RequiredDate timestamp,ShippedDate timestamp,ShipVia int,Freight double,ShipName varchar(50),ShipAddress varchar(50),ShipCity varchar(15), ShipRegion varchar(20),ShipPostalCode varchar(10),ShipCountry varchar(20),tid int)"
"create table app.order_details (OrderID int,ProductID int,UnitPrice double,Quantity smallint,Discount double,tid int)"
"create table app.products(ProductID int,ProductName varchar(100),SupplierID int,CategoryID int,QuantityPerUnit varchar(100),UnitPrice double,UnitsInStock smallint,UnitsOnOrder smallint,ReorderLevel smallint,Discontinued smallint,tid int)"
"create table app.suppliers(SupplierID int,CompanyName varchar(100),ContactName varchar(100),ContactTitle varchar(100),Address varchar(100),City varchar(100),Region varchar(100),PostalCode varchar(100),Country varchar(100),Phone varchar(100),Fax varchar(100),HomePage varchar(100),tid int) "
"create table app.territories(TerritoryID varchar(100),TerritoryDescription varchar(100),RegionID varchar(100),tid int)"
"create table app.employee_territories(EmployeeID int,TerritoryID varchar(100),tid int)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-tablesList =
"app.regions"
"app.categories"
"app.shippers"
"app.employees"
"app.customers"
"app.orders"
"app.order_details"
"app.products"
"app.suppliers"
"app.territories"
"app.employee_territories"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-dataFileLocation = ${derbyDataLocation};

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-csvFileNames =
"regions.csv"
"categories.csv"
"shippers.csv"
"employees.csv"
"customers.csv"
"orders.csv"
"order-details.csv"
"products.csv"
"suppliers.csv"
"territories.csv"
"employee-territories.csv"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-dmlTables =
"app.employees"
"app.customers"
"app.orders"
"app.order_details"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-csvLocationforLargeData = "/export/shared/QA_DATA/northWind_1G";

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertCsvFileNames =
"employees.csv"
"customers.csv"
"orders.csv"
"order-details.csv"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertStmts =
"insert into app.employees values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
"insert into app.customers values (?,?,?,?,?,?,?,?,?,?,?,?)"
"insert into app.orders values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
"insert into app.order_details values (?,?,?,?,?,?)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-updateStmts =
"UPDATE app.products SET UnitPrice = UnitPrice * 1.1 WHERE CategoryID = (SELECT max(CategoryID) from products where tid=$tid)"
"UPDATE app.products SET UnitPrice = UnitPrice * 0.9"
"UPDATE app.products SET UnitPrice = UnitPrice * 1.1, ReorderLevel = ReorderLevel + 5 WHERE CategoryID = (SELECT min(CategoryID) from products where tid=$tid)"
"UPDATE app.order_details SET UnitPrice = UnitPrice * 1.1"
"UPDATE app.order_details SET UnitPrice = UnitPrice * 1.1 WHERE ORDERID < 10450"
"UPDATE app.employees SET TitleOfCourtesy='Mr.' WHERE FIRSTNAME LIKE '%3%'"
"UPDATE app.orders SET ShipVia = ShipVia + 1 WHERE OrderID IN (SELECT max(orderID) from orders where tid=$tid)"
"UPDATE app.orders SET Freight = Freight * 1.1 WHERE Freight >= 100"
"UPDATE app.customers SET CompanyName='abc_$tid'"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-updateTables =
"products"
"products"
"products"
"order_details"
"order_details"
"employees"
"orders"
"orders"
"customers"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-deleteStmts =
"delete from app.orders where orderid IN (SELECT max(orderID) from orders where tid=$tid)"
"delete from app.customers where CustomerID IN (SELECT min(CustomerID) from customers where tid=$tid)"
"delete from app.employees where EmployeeID = 6"
"delete from app.employees where EmployeeID < 5"
"delete from app.employees where FIRSTNAME LIKE '%2'"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-deleteTables =
"orders"
"customers"
"employees"
"employees"
"employees"
;