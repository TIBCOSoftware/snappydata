io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createSchemas = "create schema app";

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createTablesStatements =
"create table app.table1 (id int, name varchar(10),code int, tid int)"

;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-tablesList =
"app.table1"
;

/*
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
*/

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-dmlTables =
"app.table1"
;

/*
io.snappydata.hydra.testDMLOps.SnappySchemaPrms-csvLocationforLargeData = "/export/shared/QA_DATA/NW_1GB_1";

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertCsvFileNames =
"products.csv"
"employees.csv"
"customers.csv"
"orders.csv"
"order_details.csv"
;
*/

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertStmtsNonDMLTables =
"insert into app.regions(RegionID,RegionDescription,tid) values (?,?,?)"
"insert into app.categories(CategoryID,CategoryName,Description,Picture,tid) values (?,?,?,?,?)"
"insert into app.shippers(ShipperID,CompanyName,Phone,tid) values (?,?,?,?)"
"insert into app.suppliers(SupplierID,CompanyName,ContactName,ContactTitle,Address,City,Region,PostalCode,Country,Phone,Fax,HomePage,tid) values (?,?,?,?,?,?,?,?,?,?,?,?,?)"
"insert into app.territories(TerritoryID,TerritoryDescription,RegionID,tid) values (?,?,?,?)"
"insert into app.employee_territories(EmployeeID,TerritoryID,tid) values (?,?,?)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertStmts =
"insert into app.products(ProductID,ProductName,SupplierID,CategoryID,QuantityPerUnit,UnitPrice,UnitsInStock,UnitsOnOrder,ReorderLevel,Discontinued,tid) values (?,?,?,?,?,?,?,?,?,?,?)"
"insert into app.employees (EmployeeID,LastName,FirstName,Title,TitleOfCourtesy,BirthDate,HireDate,Address,City,Region,PostalCode,Country,HomePhone,Extension,Photo,Notes,ReportsTo,PhotoPath,tid) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
"insert into app.customers (CustomerID,CompanyName,ContactName,ContactTitle,Address,City,Region,PostalCode,Country,Phone,Fax,tid) values (?,?,?,?,?,?,?,?,?,?,?,?)"
"insert into app.orders (OrderID,CustomerID,EmployeeID,OrderDate,RequiredDate,ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode,ShipCountry,tid) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
"insert into app.order_details (OrderID,ProductID,UnitPrice,Quantity,Discount,tid) values (?,?,?,?,?,?)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-updateStmts =
"UPDATE app.products SET UnitPrice = UnitPrice * 1.1 WHERE CategoryID = (SELECT max(CategoryID) from products where tid=$tid)"
"UPDATE app.products SET UnitPrice = UnitPrice * 0.9"
"UPDATE app.products SET UnitPrice = UnitPrice * 1.1, ReorderLevel = ReorderLevel + 5 WHERE CategoryID = (SELECT min(CategoryID) from products where tid=$tid)"
"UPDATE app.products SET Discontinued=-1 where Discontinued=1"
"UPDATE app.products SET Discontinued=1 WHERE Discontinued=-1"
"UPDATE app.order_details SET UnitPrice = UnitPrice * 1.1"
"UPDATE app.order_details SET UnitPrice = UnitPrice * 1.1 WHERE ORDERID < 10450"
"UPDATE app.order_details SET Quantity = 0 WHERE ProductID = 11"
"UPDATE app.employees SET TitleOfCourtesy='Mr.' WHERE FIRSTNAME LIKE '%3%'"
"UPDATE app.employees SET TitleOfCourtesy='Mrs.' WHERE FIRSTNAME LIKE '%3%'"
"UPDATE app.orders SET ShipVia = ShipVia + 1 WHERE OrderID IN (SELECT max(orderID) from orders where tid=$tid)"
"UPDATE app.orders SET Freight = Freight * 1.1 WHERE Freight >= 100"
"UPDATE app.customers SET CompanyName='abc_$tid'"
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
"app.products"
"app.products"
"app.products"
"app.products"
"app.products"
"app.order_details"
"app.order_details"
"app.order_details"
"app.employees"
"app.employees"
"app.orders"
"app.orders"
"app.customers"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-deleteStmts =
"delete from app.orders where orderid IN (SELECT max(orderID) from orders where tid=$tid)"
"delete from app.order_details where Quantity < 1 "
"delete from app.orders where orderid IN (SELECT min(orderID) from orders where tid=$tid)"
"delete from app.customers where CustomerID IN (SELECT min(CustomerID) from customers where tid=$tid)"
"delete from app.customers where CustomerID IN (SELECT max(CustomerID) from customers where tid=$tid)"
"delete from app.employees where EmployeeID = 6"
"delete from app.employees where EmployeeID < 5"
"delete from app.employees where FIRSTNAME LIKE '%2'"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterDeleteSelects =
"select * from app.orders"
"select * from app.order_details"
"select * from orders"
"select * from customers"
"select * from customers"
"select * from employees"
"select * from employees"
"select * from employees"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-deleteTables =
"app.orders"
"app.order_details"
"app.orders"
"app.customers"
"app.customers"
"app.employees"
"app.employees"
"app.employees"
;
