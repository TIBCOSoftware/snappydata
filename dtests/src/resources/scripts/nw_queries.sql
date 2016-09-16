elapsedtime on;
set spark.sql.shuffle.partitions=6;

SELECT * FROM Categories;
SELECT * FROM Customers;
SELECT * FROM Orders;
----- SELECTing Specific Columns 
SELECT FirstName, LastName FROM Employees;
----- Sorting By Multiple Columns
SELECT FirstName, LastName FROM Employees ORDER BY LastName;

----- Sorting By Column Position
SELECT Title, FirstName, LastName FROM Employees ORDER BY 1,3;

----- Ascending and Descending Sorts
SELECT Title, FirstName, LastName FROM Employees ORDER BY Title ASC, LastName DESC;

----- Checking for Equality
SELECT Title, FirstName, LastName FROM Employees WHERE Title = 'Sales Representative';

----- Checking for Inequality
SELECT FirstName, LastName FROM Employees WHERE Title <> 'Sales Representative';

----- Checking for Greater or Less Than
SELECT FirstName, LastName FROM Employees WHERE LastName >= 'N';

----- Checking for NULL
SELECT FirstName, LastName FROM Employees WHERE Region IS NULL;

----- WHERE and ORDER BY
SELECT FirstName, LastName FROM Employees WHERE LastName >= 'N' ORDER BY LastName DESC;

----- Using the WHERE clause to check for equality or inequality
SELECT OrderDate, ShippedDate, CustomerID, Freight FROM Orders WHERE OrderDate = '1997-05-19 00:00:00.0';

----- Using WHERE and ORDER BY Together
SELECT CompanyName, ContactName, Fax FROM Customers WHERE Fax IS NOT NULL ORDER BY CompanyName;

----- The IN Operator
SELECT TitleOfCourtesy, FirstName, LastName FROM Employees WHERE TitleOfCourtesy IN ('Ms.','Mrs.');

----- The LIKE Operator
SELECT TitleOfCourtesy, FirstName, LastName FROM Employees WHERE TitleOfCourtesy LIKE 'M%';

SELECT FirstName, LastName, BirthDate FROM Employees WHERE BirthDate BETWEEN '1950-01-01 00:00:00.0' AND '1959-12-31 23:59:59';

SELECT CONCAT(FirstName, ' ', LastName) FROM Employees;

SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal FROM Orders WHERE Freight >= 500;

SELECT SUM(Quantity) AS TotalUnits FROM Order_Details WHERE ProductID=3;

SELECT MIN(HireDate) AS FirstHireDate, MAX(HireDate) AS LastHireDate FROM Employees;

SELECT City, COUNT(EmployeeID) AS NumEmployees FROM Employees WHERE Title = 'Sales Representative' GROUP BY City HAVING COUNT(EmployeeID) > 1 ORDER BY NumEmployees;

SELECT COUNT(DISTINCT City) AS NumCities FROM Employees;

SELECT ProductID, AVG(UnitPrice) AS AveragePrice FROM Products GROUP BY ProductID HAVING AVG(UnitPrice) > 70 ORDER BY AveragePrice;

---SELECT CompanyName FROM Customers WHERE CustomerID = (SELECT CustomerID FROM Orders WHERE OrderID = 10290) --GEMFIREXD-PROPERTIES executionEngine=Spark ;

SELECT CompanyName FROM Customers --GEMFIREXD-PROPERTIES executionEngine=Spark WHERE CustomerID = (SELECT CustomerID FROM Orders WHERE OrderID = 10290);

SELECT CompanyName FROM Customers  WHERE CustomerID IN (SELECT CustomerID FROM Orders WHERE OrderDate BETWEEN '1997-01-01 00:00:00.0' AND '1997-12-31 00:00:00.0');

SELECT ProductName, SupplierID FROM Products WHERE SupplierID IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN ('Exotic Liquids', 'Grandma Kelly''s Homestead', 'Tokyo Traders'));

SELECT ProductName FROM Products WHERE CategoryID = (SELECT CategoryID FROM Categories WHERE CategoryName = 'Seafood') --GEMFIREXD-PROPERTIES executionEngine=Spark;

SELECT CompanyName  FROM Suppliers WHERE SupplierID IN (SELECT SupplierID FROM Products WHERE CategoryID = 8) --GEMFIREXD-PROPERTIES executionEngine=Spark;

SELECT CompanyName  FROM Suppliers WHERE SupplierID IN (SELECT SupplierID FROM Products  WHERE CategoryID = (SELECT CategoryID FROM Categories WHERE CategoryName = 'Seafood')) --GEMFIREXD-PROPERTIES executionEngine=Spark;

SELECT Employees.EmployeeID, Employees.FirstName, Employees.LastName, Orders.OrderID, Orders.OrderDate FROM Employees JOIN Orders ON (Employees.EmployeeID = Orders.EmployeeID) ORDER BY Orders.OrderDate;

SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName FROM Orders o JOIN Employees e ON (e.EmployeeID = o.EmployeeID) JOIN Customers c ON (c.CustomerID = o.CustomerID) WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > '1998-01-01 00:00:00.0' ORDER BY c.CompanyName;

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

SELECT * FROM orders LEFT SEMI JOIN order_details;

SELECT * FROM orders JOIN order_details;

SELECT * FROM orders LEFT JOIN order_details;

SELECT * FROM orders RIGHT JOIN order_details;

SELECT * FROM orders FULL OUTER JOIN order_details;

SELECT * FROM orders FULL JOIN order_details;

SELECT * FROM orders JOIN order_details ON Orders.OrderID = Order_Details.OrderID;

SELECT * FROM orders LEFT JOIN order_details ON Orders.OrderID = Order_Details.OrderID;

SELECT * FROM orders RIGHT JOIN order_details ON Orders.OrderID = Order_Details.OrderID;

SELECT * FROM orders FULL OUTER JOIN order_details ON Orders.OrderID = Order_Details.OrderID;

SELECT * FROM orders FULL JOIN order_details ON Orders.OrderID = Order_Details.OrderID;