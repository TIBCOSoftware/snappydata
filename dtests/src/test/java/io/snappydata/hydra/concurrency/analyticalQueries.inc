io.snappydata.hydra.cluster.SnappyPrms-analyticalQueryList =
" SELECT * FROM Categories"
" SELECT * FROM Customers"
" SELECT * FROM Orders"
" SELECT * FROM employees"
" SELECT Employees.EmployeeID, Employees.FirstName, Employees.LastName, Orders.OrderID, Orders.OrderDate FROM Employees JOIN Orders ON (Employees.EmployeeID = Orders.EmployeeID) ORDER BY Orders.OrderDate"
" SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName FROM Orders o JOIN Employees e ON (e.EmployeeID = o.EmployeeID) JOIN Customers c ON (c.CustomerID = o.CustomerID) WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > Cast ('1998-01-01' as TIMESTAMP) ORDER BY c.CompanyName"
" SELECT e.FirstName, e.LastName, o.OrderID FROM Employees e JOIN Orders o ON (e.EmployeeID = o.EmployeeID) WHERE o.RequiredDate < o.ShippedDate ORDER BY e.LastName, e.FirstName"
;
