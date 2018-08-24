elapsedtime on;
set spark.sql.shuffle.partitions=29;

SELECT CompanyName FROM Customers WHERE CustomerID = (SELECT CustomerID FROM Orders WHERE OrderID = 10290) --GEMFIREXD-PROPERTIES executionEngine=Spark
;

SELECT CompanyName FROM Customers  WHERE CustomerID IN (SELECT CustomerID FROM Orders WHERE OrderDate BETWEEN Cast('1997-01-01' as TIMESTAMP) AND Cast('1997-12-31' as TIMESTAMP));

SELECT ProductName, SupplierID FROM Products WHERE SupplierID IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN ('Exotic Liquids', 'Grandma Kelly''s Homestead', 'Tokyo Traders'));

SELECT ProductName FROM Products WHERE CategoryID = (SELECT CategoryID FROM Categories WHERE CategoryName = 'Seafood') --GEMFIREXD-PROPERTIES executionEngine=Spark
;

SELECT CompanyName  FROM Suppliers WHERE SupplierID IN (SELECT SupplierID FROM Products WHERE CategoryID = 8) --GEMFIREXD-PROPERTIES executionEngine=Spark
;

SELECT CompanyName  FROM Suppliers WHERE SupplierID IN (SELECT SupplierID FROM Products  WHERE CategoryID = (SELECT CategoryID FROM Categories WHERE CategoryName = 'Seafood')) --GEMFIREXD-PROPERTIES executionEngine=Spark
;

SELECT Employees.EmployeeID, Employees.FirstName, Employees.LastName, Orders.OrderID, Orders.OrderDate FROM Employees JOIN Orders ON (Employees.EmployeeID = Orders.EmployeeID) ORDER BY Orders.OrderDate;

SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName FROM Orders o JOIN Employees e ON (e.EmployeeID = o.EmployeeID) JOIN Customers c ON (c.CustomerID = o.CustomerID) WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > Cast ('1998-01-01' as TIMESTAMP) ORDER BY c.CompanyName;

SELECT e.FirstName, e.LastName, o.OrderID FROM Employees e JOIN Orders o ON (e.EmployeeID = o.EmployeeID) WHERE o.RequiredDate < o.ShippedDate ORDER BY e.LastName, e.FirstName;

SELECT p.ProductName, SUM(od.Quantity) AS TotalUnits FROM Order_Details od JOIN Products p ON (p.ProductID = od.ProductID) GROUP BY p.ProductName HAVING SUM(Quantity) < 200;

SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees, COUNT(DISTINCT c.CustomerID) AS numCompanies, e.City, c.City FROM Employees e JOIN Customers c ON (e.City = c.City) GROUP BY e.City, c.City ORDER BY numEmployees DESC;

SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees, COUNT(DISTINCT c.CustomerID) AS numCompanies, e.City, c.City FROM Employees e LEFT JOIN Customers c ON (e.City = c.City) GROUP BY e.City, c.City ORDER BY numEmployees DESC;

SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees, COUNT(DISTINCT c.CustomerID) AS numCompanies, e.City, c.City FROM Employees e RIGHT JOIN Customers c ON (e.City = c.City) GROUP BY e.City, c.City ORDER BY numEmployees DESC;

SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees, COUNT(DISTINCT c.CustomerID) AS numCompanies, e.City, c.City FROM Employees e FULL JOIN Customers c ON (e.City = c.City) GROUP BY e.City, c.City ORDER BY numEmployees DESC;

select s.supplierid,s.companyname,p.productid,p.productname from suppliers s join products p on(s.supplierid= p.supplierid) and s.companyname IN('Grandma Kelly''s Homestead','Tokyo Traders','Exotic Liquids');

SELECT c.customerID, o.orderID FROM customers c INNER JOIN orders o ON c.CustomerID = o.CustomerID;

SELECT order_details.OrderID,ShipCountry,UnitPrice,Quantity,Discount FROM orders INNER JOIN Order_Details ON Orders.OrderID = Order_Details.OrderID;

SELECT ShipCountry, Sum(Order_Details.UnitPrice * Quantity * Discount) AS ProductSales FROM Orders INNER JOIN Order_Details ON Orders.OrderID = Order_Details.OrderID GROUP BY ShipCountry;

SELECT * FROM orders LEFT SEMI JOIN order_details ON orders.OrderID = order_details.OrderId;

select distinct (a.ShippedDate) as ShippedDate, a.OrderID, b.Subtotal, year(a.ShippedDate) as Year from Orders a
     inner join ( select distinct OrderID, sum(UnitPrice * Quantity * (1 - Discount)) as Subtotal
     from order_details group by OrderID ) b on a.OrderID = b.OrderID
     where a.ShippedDate is not null and a.ShippedDate > Cast('1996-12-24' as TIMESTAMP) and a.ShippedDate < Cast('1997-09-30' as TIMESTAMP)
     order by ShippedDate;

select distinct a.CategoryID, a.CategoryName, b.ProductName, sum(c.ExtendedPrice) as ProductSales
     from Categories a
     inner join Products b on a.CategoryID = b.CategoryID
     inner join ( select distinct y.OrderID, y.ProductID, x.ProductName, y.UnitPrice, y.Quantity, y.Discount,
     round(y.UnitPrice * y.Quantity * (1 - y.Discount), 2) as ExtendedPrice
     from Products x
     inner join Order_Details y on x.ProductID = y.ProductID
     order by y.OrderID
     ) c on c.ProductID = b.ProductID
     inner join Orders d on d.OrderID = c.OrderID
     where d.OrderDate > Cast('1997-01-01' as TIMESTAMP) and d.OrderDate < Cast('1997-12-31' as TIMESTAMP)
     group by a.CategoryID, a.CategoryName, b.ProductName
     order by a.CategoryName, b.ProductName, ProductSales;


select c.CategoryName as Product_Category, case when s.Country in ('UK','Spain','Sweden','Germany','Norway','Denmark','Netherlands','Finland','Italy','France')
     then 'Europe' when s.Country in ('USA','Canada','Brazil') then 'America' else 'Asia-Pacific' end as Supplier_Continent, sum(p.UnitsInStock)
     as UnitsInStock from Suppliers s inner join Products p on p.SupplierID=s.SupplierID inner join Categories c on c.CategoryID=p.CategoryID
     group by c.CategoryName, case when s.Country in ('UK','Spain','Sweden','Germany','Norway', 'Denmark','Netherlands','Finland','Italy','France')
     then 'Europe' when s.Country in ('USA','Canada','Brazil') then 'America' else 'Asia-Pacific'
     end  --GEMFIREXD-PROPERTIES executionEngine=Spark
     ;

select CategoryName, format_number(sum(ProductSales), 2) as CategorySales from (select distinct a.CategoryName, b.ProductName,
     format_number(sum(c.UnitPrice * c.Quantity * (1 - c.Discount)), 2) as ProductSales, concat('Qtr ', quarter(d.ShippedDate))
     as ShippedQuarter from Categories as a inner join Products as b on a.CategoryID = b.CategoryID inner join Order_Details
     as c on b.ProductID = c.ProductID inner join Orders as d on d.OrderID = c.OrderID where d.ShippedDate > Cast('1997-01-01' as TIMESTAMP)  and d.ShippedDate < Cast('1997-12-31' as TIMESTAMP)
     group by a.CategoryName, b.ProductName, concat('Qtr ', quarter(d.ShippedDate)) order by a.CategoryName, b.ProductName, ShippedQuarter )
     as x group by CategoryName order by CategoryName;

set spark.sql.crossJoin.enabled=true;

SELECT count(*) FROM orders JOIN order_details;

SELECT count(*) FROM orders LEFT JOIN order_details;

SELECT count(*) FROM orders RIGHT JOIN order_details;

SELECT count(*) FROM orders FULL OUTER JOIN order_details;

SELECT count(*) FROM orders FULL JOIN order_details;

select distinct b.ShipName, b.ShipAddress, b.ShipCity, b.ShipRegion, b.ShipPostalCode, b.ShipCountry, b.CustomerID,
     c.CompanyName, c.Address, c.City, c.Region, c.PostalCode, c.Country, concat(d.FirstName,  ' ', d.LastName) as Salesperson,
     b.OrderID, b.OrderDate, b.RequiredDate, b.ShippedDate, a.CompanyName, e.ProductID, f.ProductName, e.UnitPrice, e.Quantity,
     e.Discount, e.UnitPrice * e.Quantity * (1 - e.Discount) as ExtendedPrice, b.Freight
     from Shippers a
     inner join Orders b on a.ShipperID = b.ShipVia
     inner join Customers c on c.CustomerID = b.CustomerID
     inner join Employees d on d.EmployeeID = b.EmployeeID
     inner join Order_Details e on b.OrderID = e.OrderID
     inner join Products f on f.ProductID = e.ProductID
     order by b.ShipName;

--This query shows how to use UNION to merge Customers and Suppliers into one result set by
--identifying them as having different relationships to Northwind Traders - Customers and Suppliers.


     select City, CompanyName, ContactName, 'Customers' as Relationship
     from Customers
     union
     select City, CompanyName, ContactName, 'Suppliers'
     from Suppliers
     order by City, CompanyName;

--In the query below, we have two sub-queries in the FROM clause and each sub-query returns a single
-- value. Because the results of the two sub-queries are basically temporary tables, we can join
--them like joining two real tables. In the SELECT clause, we simply list the two counts.

     select a.CustomersCount, b.SuppliersCount
     from
     (select count(CustomerID) as CustomersCount from customers) as a
     join
     (select count(SupplierID) as SuppliersCount from suppliers) as b;

-- The second query below uses the two values again but this time it calculates the ratio
--between customers count and suppliers count. The round and concat function are used to 
--the result.

     select concat(round(a.CustomersCount / b.SuppliersCount), ':1') as Customer_vs_Supplier_Ratio
     from
     (select count(CustomerID) as CustomersCount from customers) as a
     join
     (select count(SupplierID) as SuppliersCount from suppliers) as b;

-- This query shows how to convert order dates to the corresponding quarters. It also
--demonstrates how SUM function is used together with CASE statement to get sales for each
--quarter, where quarters are converted from OrderDate column.

--This query is commented due to SNAP-1434

     select a.ProductName,
         d.CompanyName,
         year(OrderDate) as OrderYear,
         format_number(sum(case quarter(c.OrderDate) when '1'
             then b.UnitPrice*b.Quantity*(1-b.Discount) else 0 end), 0) "Qtr_1",
         format_number(sum(case quarter(c.OrderDate) when '2'
             then b.UnitPrice*b.Quantity*(1-b.Discount) else 0 end), 0) "Qtr_2",
         format_number(sum(case quarter(c.OrderDate) when '3'
             then b.UnitPrice*b.Quantity*(1-b.Discount) else 0 end), 0) "Qtr_3",
         format_number(sum(case quarter(c.OrderDate) when '4'
             then b.UnitPrice*b.Quantity*(1-b.Discount) else 0 end), 0) "Qtr_4"
     from Products a
     inner join Order_Details b on a.ProductID = b.ProductID
     inner join Orders c on c.OrderID = b.OrderID
     inner join Customers d on d.CustomerID = c.CustomerID
     where c.OrderDate between Cast('1997-01-01' as TIMESTAMP) and Cast('1997-12-31' as TIMESTAMP)
     group by a.ProductName,
         d.CompanyName,
         year(OrderDate)
     order by a.ProductName, d.CompanyName;

SELECT CompanyName FROM Customers WHERE CustomerID = (SELECT CustomerID FROM Orders WHERE OrderID = 10295);

SELECT CompanyName FROM Customers WHERE CustomerID = (SELECT CustomerID FROM Orders WHERE OrderID = 10391);

SELECT CompanyName FROM Customers  WHERE CustomerID IN (SELECT CustomerID  FROM Orders WHERE OrderDate BETWEEN Cast('1997-09-30' as TIMESTAMP) AND  Cast('1997-12-24' as TIMESTAMP)) ;

SELECT CompanyName FROM Customers  WHERE CustomerID IN (SELECT CustomerID  FROM Orders WHERE OrderDate BETWEEN Cast('1997-10-01' as TIMESTAMP) AND  Cast('1997-12-31' as TIMESTAMP)) ;

SELECT ProductName, SupplierID FROM Products WHERE SupplierID  IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN ('Pavlova Ltd.')) ;

SELECT ProductName, SupplierID FROM Products WHERE SupplierID  IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN ('Pavlova Ltd.', 'Karkki Oy')) ;

SELECT ProductName, SupplierID FROM Products WHERE SupplierID  IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN ('Grandma Kellys Homestead')) ;

SELECT ProductName, SupplierID FROM Products WHERE SupplierID  IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN ('Exotic Liquids', 'Karkki Oy')) ;

SELECT ProductName FROM Products WHERE CategoryID = (SELECT  CategoryID FROM Categories WHERE CategoryName = 'Condiments') ;

SELECT ProductName FROM Products WHERE CategoryID = (SELECT  CategoryID FROM Categories WHERE CategoryName = 'Produce') ;

SELECT CompanyName  FROM Suppliers WHERE SupplierID IN  (SELECT SupplierID FROM Products WHERE CategoryID = 5) ;

SELECT CompanyName  FROM Suppliers WHERE SupplierID IN  (SELECT SupplierID FROM Products WHERE CategoryID = 3) ;

SELECT CompanyName  FROM Suppliers WHERE SupplierID IN (SELECT SupplierID  FROM Products  WHERE CategoryID = (SELECT CategoryID FROM Categories  WHERE CategoryName = 'Condiments')) ;

SELECT CompanyName  FROM Suppliers WHERE SupplierID IN (SELECT SupplierID  FROM Products  WHERE CategoryID = (SELECT CategoryID FROM Categories  WHERE CategoryName = 'Confections')) ;

SELECT Employees.EmployeeID, Employees.FirstName,  Employees.LastName, Orders.OrderID, Orders.OrderDate  FROM Employees JOIN Orders ON  (Employees.EmployeeID = Orders.EmployeeID)  where Orders.EmployeeID < 5  ORDER BY Orders.OrderDate ;

SELECT Employees.EmployeeID, Employees.FirstName,  Employees.LastName, Orders.OrderID, Orders.OrderDate  FROM Employees JOIN Orders ON  (Employees.EmployeeID = Orders.EmployeeID)  where Orders.EmployeeID > 5  ORDER BY Orders.OrderDate ;

SELECT Employees.EmployeeID, Employees.FirstName,  Employees.LastName, Orders.OrderID, Orders.OrderDate  FROM Employees JOIN Orders ON  (Employees.EmployeeID = Orders.EmployeeID)  where Orders.EmployeeID < 3  ORDER BY Orders.OrderDate ;

SELECT Employees.EmployeeID, Employees.FirstName,  Employees.LastName, Orders.OrderID, Orders.OrderDate  FROM Employees JOIN Orders ON  (Employees.EmployeeID = Orders.EmployeeID)  where Orders.EmployeeID > 3  ORDER BY Orders.OrderDate ;

SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName  FROM Orders o  JOIN Employees e ON (e.EmployeeID = o.EmployeeID)  JOIN Customers c ON (c.CustomerID = o.CustomerID)  WHERE o.ShippedDate < o.RequiredDate AND o.OrderDate > Cast('1997-12-01' as TIMESTAMP)  ORDER BY c.CompanyName ;

SELECT e.FirstName, e.LastName, o.OrderID  FROM Employees e JOIN Orders o ON  (e.EmployeeID = o.EmployeeID)  WHERE o.RequiredDate > o.ShippedDate  ORDER BY e.LastName, e.FirstName ;

SELECT p.ProductName, SUM(od.Quantity) AS TotalUnits  FROM Order_Details od JOIN Products p ON  (p.ProductID = od.ProductID)  GROUP BY p.ProductName  HAVING SUM(Quantity) >10 and SUM(Quantity) <100 ;

SELECT p.ProductName, SUM(od.Quantity) AS TotalUnits  FROM Order_Details od JOIN Products p ON  (p.ProductID = od.ProductID)  GROUP BY p.ProductName  HAVING SUM(Quantity) >100 and SUM(Quantity) <200 ;

SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees,  COUNT(DISTINCT c.CustomerID) AS numCompanies,  e.City as employeeCity, c.City as customerCity  FROM Employees e JOIN Customers c ON  (e.City = c.City)  where e.EmployeeID > 5   GROUP BY e.City, c.City   ORDER BY numEmployees DESC ;

SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees,  COUNT(DISTINCT c.CustomerID) AS numCompanies,  e.City as employeeCity, c.City as customerCity  FROM Employees e JOIN Customers c ON  (e.City = c.City)  where e.EmployeeID > 1   GROUP BY e.City, c.City   ORDER BY numEmployees DESC ;

select distinct (a.ShippedDate) as ShippedDate,  a.OrderID,  b.Subtotal,  year(a.ShippedDate) as Year  from Orders a  inner join  (  select distinct OrderID,  sum(UnitPrice * Quantity * (1 - Discount)) as Subtotal  from order_details  group by OrderID  ) b on a.OrderID = b.OrderID  where a.ShippedDate is not null  and a.ShippedDate > Cast('1997-02-24' as TIMESTAMP) and   a.ShippedDate < Cast('1997-09-30' as TIMESTAMP)  order by ShippedDate ;

select distinct (a.ShippedDate) as ShippedDate,  a.OrderID,  b.Subtotal,  year(a.ShippedDate) as Year  from Orders a  inner join  (  select distinct OrderID,  sum(UnitPrice * Quantity * (2 - Discount)) as Subtotal  from order_details  group by OrderID  ) b on a.OrderID = b.OrderID  where a.ShippedDate is not null  and a.ShippedDate > Cast('1996-02-24' as TIMESTAMP) and   a.ShippedDate < Cast('1996-09-30' as TIMESTAMP)  order by ShippedDate ;

select distinct b.ShipName,  b.ShipAddress,  b.ShipCity,  b.ShipRegion,  b.ShipPostalCode,  b.ShipCountry,  b.CustomerID,  c.CompanyName as custCompanyName,  c.Address,  c.City,  c.Region,  c.PostalCode,  c.Country,   concat(d.FirstName,  ' ', d.LastName) as Salesperson,  b.OrderID,  b.OrderDate,  b.RequiredDate,  b.ShippedDate,  a.CompanyName as shippersCompanyName,  e.ProductID,  f.ProductName,  e.UnitPrice,  e.Quantity,  e.Discount,  e.UnitPrice * e.Quantity * (1 - e.Discount) as ExtendedPrice,  b.Freight  from Shippers a   inner join Orders b on a.ShipperID = b.ShipVia  inner join Customers c on c.CustomerID = b.CustomerID  inner join Employees d on d.EmployeeID = b.EmployeeID  inner join Order_Details e on b.OrderID = e.OrderID  inner join Products f on f.ProductID = e.ProductID  where b.ShippedDate > Cast('1996-07-10' as TIMESTAMP)  order by b.ShipName ;

select distinct b.ShipName,  b.ShipAddress,  b.ShipCity,  b.ShipRegion,  b.ShipPostalCode,  b.ShipCountry,  b.CustomerID,  c.CompanyName as custCompanyName,  c.Address,  c.City,  c.Region,  c.PostalCode,  c.Country,   concat(d.FirstName,  ' ', d.LastName) as Salesperson,  b.OrderID,  b.OrderDate,  b.RequiredDate,  b.ShippedDate,  a.CompanyName as shippersCompanyName,  e.ProductID,  f.ProductName,  e.UnitPrice,  e.Quantity,  e.Discount,  e.UnitPrice * e.Quantity * (1 - e.Discount) as ExtendedPrice,  b.Freight  from Shippers a   inner join Orders b on a.ShipperID = b.ShipVia  inner join Customers c on c.CustomerID = b.CustomerID  inner join Employees d on d.EmployeeID = b.EmployeeID  inner join Order_Details e on b.OrderID = e.OrderID  inner join Products f on f.ProductID = e.ProductID  where b.ShippedDate > Cast('1996-07-29' as TIMESTAMP)  order by b.ShipName ;

SELECT c.customerID, o.orderID FROM customers c INNER JOIN orders o  ON c.CustomerID = o.CustomerID where c.CustomerID='LINOD' ;

SELECT c.customerID, o.orderID FROM customers c INNER JOIN orders o  ON c.CustomerID = o.CustomerID where c.CustomerID='SEVES' ;

SELECT ShipCountry,  Sum(Order_Details.UnitPrice * Quantity * Discount)  AS ProductSales FROM Orders INNER JOIN Order_Details ON  Orders.OrderID = Order_Details.OrderID where orders.OrderID > 11000 GROUP BY ShipCountry ;

SELECT ShipCountry,  Sum(Order_Details.UnitPrice * Quantity * Discount)  AS ProductSales FROM Orders INNER JOIN Order_Details ON  Orders.OrderID = Order_Details.OrderID where orders.OrderID > 11070 GROUP BY ShipCountry ;

SELECT * FROM orders LEFT SEMI JOIN order_details  ON orders.OrderID = order_details.OrderId where orders.OrderID > 11067 ;

SELECT * FROM orders LEFT SEMI JOIN order_details  ON orders.OrderID = order_details.OrderId where orders.OrderID > 11075 ;

SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate, RequiredDate,  ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode,  ShipCountry FROM orders FULL JOIN order_details  where orders.ShippedDate > Cast('1996-07-29' as TIMESTAMP) ;

SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate, RequiredDate,  ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode,  ShipCountry FROM orders FULL JOIN order_details  where orders.ShippedDate > Cast('1996-07-10' as TIMESTAMP) ;

SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate, RequiredDate,  ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode,  ShipCountry FROM orders LEFT JOIN order_details  ON Orders.OrderID = Order_Details.OrderID   where orders.ShippedDate > Cast('1996-07-10' as TIMESTAMP) ;

SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate, RequiredDate,  ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode,  ShipCountry FROM orders LEFT JOIN order_details  ON Orders.OrderID = Order_Details.OrderID   where orders.ShippedDate > Cast('1996-07-29' as TIMESTAMP) ;

select c.CategoryName as Product_Category,  case when s.Country in  ('UK','Spain','Sweden','Germany','Norway',  'Denmark','Netherlands','Finland','Italy','France')  then 'Europe'  when s.Country in ('USA','Canada','Brazil')  then 'America'  else 'Asia-Pacific'  end as Supplier_Continent,  sum(p.UnitsInStock) as UnitsInStock  from Suppliers s   inner join Products p on p.SupplierID=s.SupplierID  inner join Categories c on c.CategoryID=p.CategoryID  where s.Country IN ('USA','UK')  group by c.CategoryName,  case when s.Country in  ('UK','Spain','Sweden','Germany','Norway',  'Denmark','Netherlands','Finland','Italy','France')  then 'Europe'  when s.Country in ('USA','Canada','Brazil')  then 'America'  else 'Asia-Pacific'  end ;

select c.CategoryName as Product_Category,  case when s.Country in  ('UK','Spain','Sweden','Germany','Norway',  'Denmark','Netherlands','Finland','Italy','France')  then 'Europe'  when s.Country in ('USA','Canada','Brazil')  then 'America'  else 'Asia-Pacific'  end as Supplier_Continent,  sum(p.UnitsInStock) as UnitsInStock  from Suppliers s   inner join Products p on p.SupplierID=s.SupplierID  inner join Categories c on c.CategoryID=p.CategoryID  where s.Country IN ('Canada','France')  group by c.CategoryName,  case when s.Country in  ('UK','Spain','Sweden','Germany','Norway',  'Denmark','Netherlands','Finland','Italy','France')  then 'Europe'  when s.Country in ('USA','Canada','Brazil')  then 'America'  else 'Asia-Pacific'  end ;

select CategoryName, format_number(sum(ProductSales), 2) as CategorySales  from  (  select distinct a.CategoryName,  b.ProductName,  format_number(sum(c.UnitPrice * c.Quantity * (1 - c.Discount)), 2) as ProductSales,  concat('Qtr ', quarter(d.ShippedDate)) as ShippedQuarter  from Categories as a  inner join Products as b on a.CategoryID = b.CategoryID  inner join Order_Details as c on b.ProductID = c.ProductID  inner join Orders as d on d.OrderID = c.OrderID  where d.ShippedDate > Cast('1996-12-01' as TIMESTAMP) and  d.ShippedDate < Cast('1997-07-10' as TIMESTAMP)  group by a.CategoryName,  b.ProductName,  concat('Qtr ', quarter(d.ShippedDate))  order by a.CategoryName,  b.ProductName,  ShippedQuarter  ) as x  group by CategoryName  order by CategoryName ;

select CategoryName, format_number(sum(ProductSales), 2) as CategorySales  from  (  select distinct a.CategoryName,  b.ProductName,  format_number(sum(c.UnitPrice * c.Quantity * (1 - c.Discount)), 2) as ProductSales,  concat('Qtr ', quarter(d.ShippedDate)) as ShippedQuarter  from Categories as a  inner join Products as b on a.CategoryID = b.CategoryID  inner join Order_Details as c on b.ProductID = c.ProductID  inner join Orders as d on d.OrderID = c.OrderID  where d.ShippedDate < Cast('1998-01-01' as TIMESTAMP) and  d.ShippedDate > Cast('1996-07-29' as TIMESTAMP)  group by a.CategoryName,  b.ProductName,  concat('Qtr ', quarter(d.ShippedDate))  order by a.CategoryName,  b.ProductName,  ShippedQuarter  ) as x  group by CategoryName  order by CategoryName ;

select CategoryName, format_number(sum(ProductSales), 2) as CategorySales  from  (  select distinct a.CategoryName,  b.ProductName,  format_number(sum(c.UnitPrice * c.Quantity * (1 - c.Discount)), 2) as ProductSales,  concat('Qtr ', quarter(d.ShippedDate)) as ShippedQuarter  from Categories as a  inner join Products as b on a.CategoryID = b.CategoryID  inner join Order_Details as c on b.ProductID = c.ProductID  inner join Orders as d on d.OrderID = c.OrderID  where d.ShippedDate < Cast('1998-12-01' as TIMESTAMP) and  d.ShippedDate > Cast('1996-07-10' as TIMESTAMP)  group by a.CategoryName,  b.ProductName,  concat('Qtr ', quarter(d.ShippedDate))  order by a.CategoryName,  b.ProductName,  ShippedQuarter  ) as x  group by CategoryName  order by CategoryName ;
