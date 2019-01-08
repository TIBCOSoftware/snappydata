sql.SQLPrms-createSchemas = "create schema gemfire1";

sql.SQLPrms-createTablesStatements =
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

io.snappydata.hydra.snapshotIsolation.SnapshotIsolationPrms-tablesList =
"gemfire1.regions"
"gemfire1.categories"
"gemfire1.shippers"
"gemfire1.employees"
"gemfire1.customers"
"gemfire1.orders"
"gemfire1.order_details"
"gemfire1.products"
"gemfire1.suppliers"
"gemfire1.territories"
"gemfire1.employee_territories"
;

io.snappydata.hydra.snapshotIsolation.SnapshotIsolationPrms-dataFileLocation = ${derbyDataLocation};

io.snappydata.hydra.snapshotIsolation.SnapshotIsolationPrms-csvFileNames =
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

io.snappydata.hydra.snapshotIsolation.SnapshotIsolationPrms-selectStmts = ONEOF
"SELECT CategoryID,CategoryName,Description FROM gemfire1.Categories"
"SELECT * FROM gemfire1.Customers"
"SELECT * FROM gemfire1.Orders"
"SELECT FirstName, LastName FROM gemfire1.Employees"
"SELECT FirstName, LastName FROM gemfire1.Employees ORDER BY LastName"
"SELECT Title, FirstName, LastName FROM gemfire1.Employees ORDER BY 1,3"
"SELECT Title, FirstName, LastName FROM gemfire1.Employees ORDER BY Title ASC, LastName DESC"
"SELECT Title, FirstName, LastName FROM gemfire1.Employees WHERE Title = 'Sales Representative'"
"SELECT FirstName, LastName FROM gemfire1.Employees WHERE Title <> 'Sales Representative'"
"SELECT FirstName, LastName FROM gemfire1.Employees  WHERE LastName >= 'N'"
"SELECT FirstName, LastName FROM gemfire1.Employees WHERE Region IS NULL"
"SELECT FirstName, LastName FROM gemfire1.Employees WHERE LastName >= 'N' ORDER BY LastName DESC"
"SELECT CompanyName, ContactName, Fax FROM gemfire1.Customers WHERE Fax IS NOT NULL ORDER BY CompanyName"
"SELECT TitleOfCourtesy, FirstName, LastName FROM gemfire1.Employees WHERE TitleOfCourtesy IN ('Ms.','Mrs.')"
"SELECT TitleOfCourtesy, FirstName, LastName FROM gemfire1.Employees WHERE TitleOfCourtesy LIKE 'M%'"
"SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal FROM gemfire1.Orders WHERE Freight >= 500"
//"SELECT SUM(Quantity) AS TotalUnits FROM Order_Details WHERE ProductID=3"
"SELECT MIN(HireDate) AS FirstHireDate, MAX(HireDate) AS LastHireDate FROM gemfire1.Employees"
//"SELECT City, COUNT(EmployeeID) AS NumEmployees FROM gemfire1.Employees  WHERE Title = 'Sales Representative' GROUP BY City HAVING COUNT(EmployeeID) > 1 ORDER BY NumEmployees"
//"SELECT COUNT(DISTINCT City) AS NumCities FROM gemfire1.Employees"
//"SELECT ProductID, AVG(UnitPrice) AS AveragePrice FROM Products  GROUP BY ProductID  HAVING AVG(UnitPrice) > 70 ORDER BY AveragePrice"
//"SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees, COUNT(DISTINCT c.CustomerID) AS numCompanies, e.City as employeeCity, c.City as customerCity FROM Employees e JOIN Customers c ON (e.City = c.City) GROUP BY e.City, c.City  ORDER BY numEmployees DESC"
//"SELECT p.ProductName, SUM(od.Quantity) AS TotalUnits FROM Order_Details od JOIN Products p ON (p.ProductID = od.ProductID) GROUP BY p.ProductName HAVING SUM(Quantity) < 200"
//"SELECT ShipCountry, Sum(Order_Details.UnitPrice * Quantity * Discount) AS ProductSales FROM Orders INNER JOIN Order_Details ON Orders.OrderID = Order_Details.OrderID GROUP BY ShipCountry"
"SELECT CompanyName FROM gemfire1.Customers WHERE CustomerID = (SELECT CustomerID FROM gemfire1.Orders WHERE OrderID = 10290)"
"SELECT ProductName, SupplierID FROM gemfire1.Products WHERE SupplierID IN (SELECT SupplierID FROM gemfire1.Suppliers WHERE CompanyName IN('Exotic Liquids', 'Grandma Kellys Homestead', 'Tokyo Traders'))"
"SELECT ProductName FROM gemfire1.Products WHERE CategoryID = (SELECT CategoryID FROM gemfire1.Categories WHERE CategoryName = 'Seafood')"
"SELECT CompanyName  FROM gemfire1.Suppliers WHERE SupplierID IN (SELECT SupplierID FROM gemfire1.Products WHERE CategoryID = 8)"
//"SELECT CompanyName  FROM Suppliers WHERE SupplierID IN (SELECT SupplierID FROM Products  WHERE CategoryID = (SELECT CategoryID FROM Categories WHERE CategoryName = 'Seafood'))"
"SELECT e.EmployeeID, e.FirstName, e.LastName, o.OrderID, o.OrderDate FROM gemfire1.Employees e JOIN gemfire1.Orders o ON (e.EmployeeID = o.EmployeeID) ORDER BY o.OrderDate"
"SELECT e.FirstName, e.LastName, o.OrderID FROM gemfire1.Employees e JOIN gemfire1.Orders o ON (e.EmployeeID = o.EmployeeID) WHERE o.RequiredDate < o.ShippedDate ORDER BY e.LastName, e.FirstName"
"select s.supplierid,s.companyname,p.productid,p.productname from gemfire1.suppliers s join gemfire1.products p on(s.supplierid= p.supplierid) and s.companyname IN('Grandma Kellys Homestead','Tokyo Traders','Exotic Liquids')"
"SELECT c.customerID, o.orderID FROM gemfire1.customers c INNER JOIN gemfire1.orders o ON c.CustomerID = o.CustomerID"
"SELECT OrderDate, ShippedDate, CustomerID, Freight FROM gemfire1.Orders WHERE OrderDate = '1997-05-19 00:00:00.000'"
"SELECT FirstName, LastName, BirthDate FROM gemfire1.Employees WHERE BirthDate BETWEEN '1950-01-01 00:00:00.000' AND '1959-12-31 23:59:59'"
"SELECT CompanyName FROM gemfire1.Customers  WHERE CustomerID IN (SELECT CustomerID FROM gemfire1.Orders WHERE OrderDate BETWEEN '1997-01-01 00:00:00.000' AND '1997-12-31 00:00:00.000')"
"SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName FROM gemfire1.Orders o JOIN gemfire1.Employees e ON (e.EmployeeID = o.EmployeeID) JOIN gemfire1.Customers c ON (c.CustomerID = o.CustomerID) WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > '1998-01-01 00:00:00.000' ORDER BY c.CompanyName"
//"select distinct (a.ShippedDate) as ShippedDate, a.OrderID, b.Subtotal, year(a.ShippedDate) as Year_ from gemfire1.Orders a inner join ( select distinct OrderID, sum(UnitPrice * Quantity * (1 - Discount)) as Subtotal from order_details group by OrderID ) b on a.OrderID = b.OrderID where a.ShippedDate is not null and a.ShippedDate > '1996-12-24 00:00:00.000' and  a.ShippedDate < '1997-09-30 00:00:00.000' order by ShippedDate"
//"select distinct a.CategoryID, a.CategoryName, b.ProductName, sum(c.ExtendedPrice) as ProductSales from gemfire1.Categories a  inner join Products b on a.CategoryID = b.CategoryID inner join ( select distinct y.OrderID, y.ProductID, x.ProductName, y.UnitPrice, y.Quantity, y.Discount, round(y.UnitPrice * y.Quantity * (1 - y.Discount), 2) as ExtendedPrice from Products x inner join Order_Details y on x.ProductID = y.ProductID order by y.OrderID ) c on c.ProductID = b.ProductID inner join Orders d on d.OrderID = c.OrderID where d.OrderDate > '1997-01-01 00:00:00.000' and d.OrderDate < '1997-12-31 00:00:00.000' group by a.CategoryID, a.CategoryName, b.ProductName order by a.CategoryName, b.ProductName, ProductSales"
//"SELECT FirstName || ' ' || LastName FROM Employees"
//"SELECT * FROM gemfire1.orders LEFT SEMI JOIN order_details ON orders.OrderID = order_details.OrderId"
//"SELECT * FROM gemfire1.orders LEFT SEMI JOIN order_details"
//"select c.CategoryName as Product_Category, case when s.Country in ('UK','Spain','Sweden','Germany','Norway', 'Denmark','Netherlands','Finland','Italy','France') then 'Europe' when s.Country in ('USA','Canada','Brazil') then 'America' else 'Asia-Pacific' end as Supplier_Continent, sum(p.UnitsInStock) as UnitsInStock from Suppliers s  inner join Products p on p.SupplierID=s.SupplierID inner join Categories c on c.CategoryID=p.CategoryID group by c.CategoryName, case when s.Country in ('UK','Spain','Sweden','Germany','Norway', 'Denmark','Netherlands','Finland','Italy','France') then 'Europe' when s.Country in ('USA','Canada','Brazil') then 'America' else 'Asia-Pacific' end"
//"select CategoryName, format_number(sum(ProductSales), 2) as CategorySales from ( select distinct a.CategoryName, b.ProductName, format_number(sum(c.UnitPrice * c.Quantity * (1 - c.Discount)), 2) as ProductSales, concat('Qtr ', quarter(d.ShippedDate)) as ShippedQuarter from Categories as a inner join Products as b on a.CategoryID = b.CategoryID inner join Order_Details as c on b.ProductID = c.ProductID inner join Orders as d on d.OrderID = c.OrderID where d.ShippedDate > '1997-01-01 00:00:00.000' and d.ShippedDate < '1997-12-31 00:00:00.000' group by a.CategoryName, b.ProductName, concat('Qtr ', quarter(d.ShippedDate)) order by a.CategoryName, b.ProductName, ShippedQuarter ) as x group by CategoryName order by CategoryName"
FOENO
;

io.snappydata.hydra.snapshotIsolation.SnapshotIsolationPrms-dmlTables =
"gemfire1.employees"
"gemfire1.customers"
"gemfire1.orders"
"gemfire1.order_details"
;

io.snappydata.hydra.snapshotIsolation.SnapshotIsolationPrms-csvLocationforLargeData = "/export/shared/QA_DATA/northWind_1G";

io.snappydata.hydra.snapshotIsolation.SnapshotIsolationPrms-insertCsvFileNames =
"employees.csv"
"customers.csv"
"orders.csv"
"order-details.csv"
;

io.snappydata.hydra.snapshotIsolation.SnapshotIsolationPrms-insertStmts =
"insert into gemfire1.employees values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
"insert into gemfire1.customers values (?,?,?,?,?,?,?,?,?,?,?,?)"
"insert into gemfire1.orders values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
"insert into gemfire1.order_details values (?,?,?,?,?,?)"
;

io.snappydata.hydra.snapshotIsolation.SnapshotIsolationPrms-updateStmts = ONEOF
"UPDATE gemfire1.products SET UnitPrice = UnitPrice * 1.1 WHERE CategoryID = 2"
"UPDATE gemfire1.products SET UnitPrice = UnitPrice * 0.9"
"UPDATE gemfire1.products SET UnitPrice = UnitPrice * 1.1, ReorderLevel = ReorderLevel + 5 WHERE CategoryID = 1"
"UPDATE gemfire1.order_details SET UnitPrice = UnitPrice * 1.1"
"UPDATE gemfire1.order_details SET UnitPrice = UnitPrice * 1.1 WHERE ORDERID < 10450"
"UPDATE gemfire1.employees SET TitleOfCourtesy='Mr.' WHERE FIRSTNAME LIKE '%31'"
"UPDATE gemfire1.orders SET ShipVia = ShipVia + 1 WHERE OrderID = 10290"
"UPDATE gemfire1.orders SET Freight = Freight * 1.1 WHERE Freight >= 500"
FOENO
;

io.snappydata.hydra.snapshotIsolation.SnapshotIsolationPrms-deleteStmts = ONEOF
"delete from gemfire1.employees where EmployeeID = 6"
"delete from gemfire1.employees where EmployeeID < 5"
"delete from gemfire1.employees where FIRSTNAME LIKE '%2'"
FOENO
;

