io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createSchemas = "create schema gemfire1";

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createTablesStatements =
"create table gemfire1.regions (RegionID int,RegionDescription varchar(100),tid int)"
"create table gemfire1.categories (CategoryID int,CategoryName varchar(100),Description varchar(100),Picture varchar(300),tid int)"
"create table gemfire1.shippers (ShipperID int not null,CompanyName varchar(100) not null,Phone varchar(100),tid int)"
"create table gemfire1.employees(EmployeeID int,LastName varchar(100),FirstName varchar(100),Title varchar(100),TitleOfCourtesy varchar(100),BirthDate timestamp,HireDate timestamp,Address varchar(100),City varchar(100),Region varchar(100),PostalCode varchar(100),Country varchar(100), HomePhone varchar(100),Extension varchar(100),Photo varchar(300),Notes varchar(300),ReportsTo int,PhotoPath varchar(100),tid int)"
"create table gemfire1.customers(CustomerID varchar(100),CompanyName varchar(100),ContactName varchar(100),ContactTitle varchar(100),Address varchar(100),City varchar(100),Region varchar(100),PostalCode varchar(100),Country varchar(100),Phone varchar(100),Fax varchar(100),tid int)"
"create table gemfire1.orders (OrderID int,CustomerID varchar(20),EmployeeID int,OrderDate timestamp,RequiredDate timestamp,ShippedDate timestamp,ShipVia int,Freight double,ShipName varchar(50),ShipAddress varchar(50),ShipCity varchar(15), ShipRegion varchar(20),ShipPostalCode varchar(10),ShipCountry varchar(20),tid int)"
"create table gemfire1.order_details (OrderID int,ProductID int,UnitPrice double,Quantity smallint,Discount double,tid int)"
"create table gemfire1.products(ProductID int,ProductName varchar(100),SupplierID int,CategoryID int,QuantityPerUnit varchar(100),UnitPrice double,UnitsInStock smallint,UnitsOnOrder smallint,ReorderLevel smallint,Discontinued smallint,tid int)"
"create table gemfire1.suppliers(SupplierID int,CompanyName varchar(100),ContactName varchar(100),ContactTitle varchar(100),Address varchar(100),City varchar(100),Region varchar(100),PostalCode varchar(100),Country varchar(100),Phone varchar(100),Fax varchar(100),HomePage varchar(100),tid int) "
"create table gemfire1.territories(TerritoryID varchar(100),TerritoryDescription varchar(100),RegionID varchar(100),tid int)"
"create table gemfire1.employee_territories(EmployeeID int,TerritoryID varchar(100),tid int)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-tablesList =
"gemfire1.regions"
"gemfire1.categories"
"gemfire1.shippers"
"gemfire1.suppliers"
"gemfire1.territories"
"gemfire1.employee_territories"
"gemfire1.products"
"gemfire1.employees"
"gemfire1.customers"
"gemfire1.orders"
"gemfire1.order_details"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-dataFileLocation = "/export/shared/QA_DATA/NW_1GB";

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-csvFileNames =
"regions.csv"
"categories.csv"
"shippers.csv"
"suppliers.csv"
"territories.csv"
"employee_territories.csv"
"products.csv"
"employees.csv"
"customers.csv"
"orders.csv"
"order_details.csv"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-loadDataInParts=false;

//io.snappydata.hydra.testDMLOps.SnappySchemaPrms-numPartsForDataFiles=3;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-dmlTables =
"gemfire1.products"
"gemfire1.employees"
"gemfire1.customers"
"gemfire1.orders"
"gemfire1.order_details"
;

/*
io.snappydata.hydra.testDMLOps.SnappySchemaPrms-selectOrderbyClause =
" order by ProductID"
" order by EmployeeID"
" order by CustomerID"
" order by OrderID"
" order by OrderID"
;
*/

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-csvLocationforLargeData = "/export/shared/QA_DATA/NW_1GB_1";

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertCsvFileNames =
"products.csv"
"employees.csv"
"customers.csv"
"orders.csv"
"order_details.csv"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertStmtsNonDMLTables =
"insert into gemfire1.regions(RegionID,RegionDescription,tid) values (?,?,?)"
"insert into gemfire1.categories(CategoryID,CategoryName,Description,Picture,tid) values (?,?,?,?,?)"
"insert into gemfire1.shippers(ShipperID,CompanyName,Phone,tid) values (?,?,?,?)"
"insert into gemfire1.suppliers(SupplierID,CompanyName,ContactName,ContactTitle,Address,City,Region,PostalCode,Country,Phone,Fax,HomePage,tid) values (?,?,?,?,?,?,?,?,?,?,?,?,?)"
"insert into gemfire1.territories(TerritoryID,TerritoryDescription,RegionID,tid) values (?,?,?,?)"
"insert into gemfire1.employee_territories(EmployeeID,TerritoryID,tid) values (?,?,?)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertStmts =
"insert into gemfire1.products(ProductID,ProductName,SupplierID,CategoryID,QuantityPerUnit,UnitPrice,UnitsInStock,UnitsOnOrder,ReorderLevel,Discontinued,tid) values (?,?,?,?,?,?,?,?,?,?,?)"
"insert into gemfire1.employees (EmployeeID,LastName,FirstName,Title,TitleOfCourtesy,BirthDate,HireDate,Address,City,Region,PostalCode,Country,HomePhone,Extension,Photo,Notes,ReportsTo,PhotoPath,tid) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
"insert into gemfire1.customers (CustomerID,CompanyName,ContactName,ContactTitle,Address,City,Region,PostalCode,Country,Phone,Fax,tid) values (?,?,?,?,?,?,?,?,?,?,?,?)"
"insert into gemfire1.orders (OrderID,CustomerID,EmployeeID,OrderDate,RequiredDate,ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode,ShipCountry,tid) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
"insert into gemfire1.order_details (OrderID,ProductID,UnitPrice,Quantity,Discount,tid) values (?,?,?,?,?,?)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-updateStmts =
"UPDATE gemfire1.products SET UnitPrice = UnitPrice * 1.1 WHERE CategoryID = (SELECT max(CategoryID) from products where tid=$tid)"
"UPDATE gemfire1.products SET UnitPrice = UnitPrice * 0.9"
"UPDATE gemfire1.products SET UnitPrice = UnitPrice * 1.1, ReorderLevel = ReorderLevel + 5 WHERE CategoryID = (SELECT min(CategoryID) from products where tid=$tid)"
"UPDATE gemfire1.products SET Discontinued=-1 where Discontinued=1"
"UPDATE gemfire1.products SET Discontinued=1 WHERE Discontinued=-1"
"UPDATE gemfire1.order_details SET UnitPrice = UnitPrice * 1.1"
"UPDATE gemfire1.order_details SET UnitPrice = UnitPrice * 1.1 WHERE ORDERID < 10450"
"UPDATE gemfire1.order_details SET Quantity = 0 WHERE ProductID = 11"
"UPDATE gemfire1.employees SET TitleOfCourtesy='Mr.' WHERE FIRSTNAME LIKE '%3%'"
"UPDATE gemfire1.employees SET TitleOfCourtesy='Mrs.' WHERE FIRSTNAME LIKE '%3%'"
"UPDATE gemfire1.orders SET ShipVia = ShipVia + 1 WHERE OrderID IN (SELECT max(orderID) from orders where tid=$tid)"
"UPDATE gemfire1.orders SET Freight = Freight * 1.1 WHERE Freight >= 100"
"UPDATE gemfire1.customers SET CompanyName='abc_$tid'"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterUpdateSelects =
"select ProductID,CategoryID,UnitPrice from products"
"select ProductID,UnitPrice from products"
"select ProductID,CategoryID,UnitPrice, ReorderLevel from products"
"select ProductID,Discontinued from products where Discontinued=-1"
"select ProductID,Discontinued from products where ReorderLevel=1"
"select orderID,UnitPrice from order_details"
"select orderID,UnitPrice from order_details"
"select orderID,Quantity from order_details where Quantity=0"
"select EmployeeID,TitleOfCourtesy from employees where TitleOfCourtesy='Mr.'"
"select EmployeeID,TitleOfCourtesy from employees where TitleOfCourtesy='Mrs.'"
"select OrderID,ShipVia from orders"
"select OrderID,Freight from orders"
"select customerID,companyname from customers"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-updateTables =
"gemfire1.products"
"gemfire1.products"
"gemfire1.products"
"gemfire1.products"
"gemfire1.products"
"gemfire1.order_details"
"gemfire1.order_details"
"gemfire1.order_details"
"gemfire1.employees"
"gemfire1.employees"
"gemfire1.orders"
"gemfire1.orders"
"gemfire1.customers"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-deleteStmts =
"delete from gemfire1.orders where orderid IN (SELECT max(orderID) from orders where tid=$tid)"
"delete from gemfire1.order_details where Quantity < 1 "
"delete from gemfire1.orders where orderid IN (SELECT min(orderID) from orders where tid=$tid)"
"delete from gemfire1.customers where CustomerID IN (SELECT min(CustomerID) from customers where tid=$tid)"
"delete from gemfire1.customers where CustomerID IN (SELECT max(CustomerID) from customers where tid=$tid)"
"delete from gemfire1.employees where EmployeeID = 6"
"delete from gemfire1.employees where EmployeeID < 5"
"delete from gemfire1.employees where FIRSTNAME LIKE '%2'"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterDeleteSelects =
"select * from gemfire1.orders"
"select * from gemfire1.order_details"
"select * from gemfire1.orders"
"select * from gemfire1.customers"
"select * from gemfire1.customers"
"select * from gemfire1.employees"
"select * from gemfire1.employees"
"select * from gemfire1.employees"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-deleteTables =
"gemfire1.orders"
"gemfire1.order_details"
"gemfire1.orders"
"gemfire1.customers"
"gemfire1.customers"
"gemfire1.employees"
"gemfire1.employees"
"gemfire1.employees"
;
