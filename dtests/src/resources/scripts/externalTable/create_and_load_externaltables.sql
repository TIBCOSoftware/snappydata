DROP TABLE IF EXISTS regions;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE regions
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/regions.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');


DROP TABLE IF EXISTS categories;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE categories
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/categories.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');

DROP TABLE IF EXISTS shippers;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE shippers
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/shippers.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');


DROP TABLE IF EXISTS employees;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE employees
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/employees.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');

DROP TABLE IF EXISTS customers;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE customers
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/customers.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');

DROP TABLE IF EXISTS orders;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE orders
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/orders.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');

DROP TABLE IF EXISTS order_details;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE order_details
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/order-details.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');

DROP TABLE IF EXISTS products;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE products
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/products.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');

DROP TABLE IF EXISTS suppliers;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE suppliers
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/suppliers.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');

DROP TABLE IF EXISTS territories;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE territories
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/territories.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');

DROP TABLE IF EXISTS employee_territories;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE employee_territories
    USING com.databricks.spark.csv OPTIONS(path ':dataLocation/employee-territories.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
