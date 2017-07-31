SELECT count(*) FROM user2.employees;
SELECT count(*) FROM user2.categories;
SELECT * FROM user2.categories;
SELECT * FROM user2.customers;
SELECT * FROM user2.orders;
select * from user2.suppliers;
SELECT ProductID, AVG(UnitPrice) AS AveragePrice FROM user2.products GROUP BY ProductID HAVING AVG(UnitPrice) > 70 ORDER BY AveragePrice;
