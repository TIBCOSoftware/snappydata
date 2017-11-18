DROP TABLE IF EXISTS regions;
DROP TABLE IF EXISTS staging_regions;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_regions
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/regions.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE regions USING row OPTIONS(PERSISTENT ':persistenceMode') AS (SELECT RegionID, RegionDescription FROM staging_regions);

DROP TABLE IF EXISTS categories;
DROP TABLE IF EXISTS staging_categories;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_categories
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/categories.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE categories USING row OPTIONS(PERSISTENT ':persistenceMode') AS (SELECT CategoryID, CategoryName,
Description, Picture FROM staging_categories);

DROP TABLE IF EXISTS shippers;
DROP TABLE IF EXISTS staging_shippers;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_shippers
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/shippers.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
create table shippers USING row OPTIONS(PERSISTENT ':persistenceMode') AS (SELECT ShipperID, CompanyName, Phone FROM staging_shippers);

DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS staging_employees;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_employees
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/employees.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE employees USING row OPTIONS(PERSISTENT ':persistenceMode') AS (SELECT EmployeeID, LastName,  FirstName, Title,
 TitleOfCourtesy, BirthDate, HireDate, Address, City, Region, PostalCode, Country, HomePhone, Extension, Photo,
 Notes, ReportsTo, PhotoPath FROM staging_employees);

DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS staging_customers;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_customers
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/customers.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE customers USING row OPTIONS(PERSISTENT ':persistenceMode') AS (SELECT CustomerID, CompanyName, ContactName, ContactTitle,
Address, City, Region, PostalCode, Country, Phone, Fax FROM staging_customers);

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS staging_orders;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_orders
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/orders.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE orders USING row OPTIONS(PERSISTENT ':persistenceMode') AS (SELECT OrderID, CustomerID, EmployeeID, OrderDate,
   RequiredDate, ShippedDate, ShipVia, Freight, ShipName,
   ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry FROM staging_orders);

DROP TABLE IF EXISTS order_details;
DROP TABLE IF EXISTS staging_order_details;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_order_details
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/order-details.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE order_details USING row OPTIONS(PERSISTENT ':persistenceMode') AS (SELECT OrderID, ProductID, UnitPrice,
Quantity, Discount FROM staging_order_details);

DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS staging_products;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_products
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/products.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE products USING row OPTIONS(PERSISTENT ':persistenceMode') AS (SELECT ProductID, ProductName, SupplierID, CategoryID,
QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder,
ReorderLevel, Discontinued FROM staging_products);

DROP TABLE IF EXISTS suppliers;
DROP TABLE IF EXISTS staging_suppliers;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_suppliers
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/suppliers.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE suppliers USING row OPTIONS(PERSISTENT ':persistenceMode') AS (SELECT SupplierID, CompanyName, ContactName,
ContactTitle, Address, City, Region, PostalCode,
Country, Phone, Fax, HomePage FROM staging_suppliers);

DROP TABLE IF EXISTS territories;
DROP TABLE IF EXISTS staging_territories;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_territories
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/territories.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE territories USING row OPTIONS(PERSISTENT ':persistenceMode') AS (SELECT TerritoryID, TerritoryDescription, RegionID
FROM staging_territories);


DROP TABLE IF EXISTS employee_territories;
DROP TABLE IF EXISTS staging_employee_territories;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_employee_territories
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/employee-territories.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');

CREATE TABLE employee_territories USING row OPTIONS(PERSISTENT ':persistenceMode') AS (SELECT EmployeeID, TerritoryID
FROM staging_employee_territories);
