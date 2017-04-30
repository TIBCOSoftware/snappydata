io.snappydata.hydra.cluster.SnappyPrms-pointLookUpQueryList =
" SELECT SUM(Quantity) AS TotalUnits FROM Order_Details WHERE ProductID=3"
" SELECT CompanyName, ContactName, Fax FROM Customers WHERE Fax IS NOT NULL ORDER BY CompanyName"
" SELECT City, COUNT(EmployeeID) AS NumEmployees FROM Employees WHERE Title = 'Sales Representative' GROUP BY City HAVING COUNT(EmployeeID) > 1 ORDER BY NumEmployees"
" SELECT CompanyName FROM Customers WHERE CustomerID = (SELECT CustomerID FROM Orders WHERE OrderID = 10290) --GEMFIREXD-PROPERTIES executionEngine=Spark"
" SELECT ProductName, SupplierID FROM Products WHERE SupplierID IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN ('Exotic Liquids', 'Grandma Kelly''s Homestead', 'Tokyo Traders'))"
;
