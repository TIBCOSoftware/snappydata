io.snappydata.hydra.cluster.SnappyPrms-analyticalQueryList =
" select distinct a.CategoryID, a.CategoryName, b.ProductName, sum(c.ExtendedPrice) as ProductSales
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
       order by a.CategoryName, b.ProductName, ProductSales"
" select c.CategoryName as Product_Category, case when s.Country in ('UK','Spain','Sweden','Germany','Norway','Denmark','Netherlands','Finland','Italy','France')
       then 'Europe' when s.Country in ('USA','Canada','Brazil') then 'America' else 'Asia-Pacific' end as Supplier_Continent, sum(p.UnitsInStock)
       as UnitsInStock from Suppliers s inner join Products p on p.SupplierID=s.SupplierID inner join Categories c on c.CategoryID=p.CategoryID
       group by c.CategoryName, case when s.Country in ('UK','Spain','Sweden','Germany','Norway', 'Denmark','Netherlands','Finland','Italy','France')
       then 'Europe' when s.Country in ('USA','Canada','Brazil') then 'America' else 'Asia-Pacific'
       end  --GEMFIREXD-PROPERTIES executionEngine=Spark"
" select CategoryName, format_number(sum(ProductSales), 2) as CategorySales from (select distinct a.CategoryName, b.ProductName,
       format_number(sum(c.UnitPrice * c.Quantity * (1 - c.Discount)), 2) as ProductSales, concat('Qtr ', quarter(d.ShippedDate))
       as ShippedQuarter from Categories as a inner join Products as b on a.CategoryID = b.CategoryID inner join Order_Details
       as c on b.ProductID = c.ProductID inner join Orders as d on d.OrderID = c.OrderID where d.ShippedDate > Cast('1997-01-01' as TIMESTAMP)  and d.ShippedDate < Cast('1997-12-31' as TIMESTAMP)
       group by a.CategoryName, b.ProductName, concat('Qtr ', quarter(d.ShippedDate)) order by a.CategoryName, b.ProductName, ShippedQuarter )
       as x group by CategoryName order by CategoryName"
" select distinct (a.ShippedDate) as ShippedDate, a.OrderID, b.Subtotal, year(a.ShippedDate) as Year from Orders a
       inner join ( select distinct OrderID, sum(UnitPrice * Quantity * (1 - Discount)) as Subtotal
       from order_details group by OrderID ) b on a.OrderID = b.OrderID
       where a.ShippedDate is not null and a.ShippedDate > Cast('1996-12-24' as TIMESTAMP) and a.ShippedDate < Cast('1997-09-30' as TIMESTAMP)
       order by ShippedDate"
" select distinct b.ShipName, b.ShipAddress, b.ShipCity, b.ShipRegion, b.ShipPostalCode, b.ShipCountry, b.CustomerID,
       c.CompanyName, c.Address, c.City, c.Region, c.PostalCode, c.Country, concat(d.FirstName,  ' ', d.LastName) as Salesperson,
       b.OrderID, b.OrderDate, b.RequiredDate, b.ShippedDate, a.CompanyName, e.ProductID, f.ProductName, e.UnitPrice, e.Quantity,
       e.Discount, e.UnitPrice * e.Quantity * (1 - e.Discount) as ExtendedPrice, b.Freight
       from Shippers a
       inner join Orders b on a.ShipperID = b.ShipVia
       inner join Customers c on c.CustomerID = b.CustomerID
       inner join Employees d on d.EmployeeID = b.EmployeeID
       inner join Order_Details e on b.OrderID = e.OrderID
       inner join Products f on f.ProductID = e.ProductID
       order by b.ShipName"
;
