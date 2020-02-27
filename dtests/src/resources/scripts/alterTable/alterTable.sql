ALTER TABLE orders ADD FirstName String;

describe orders;

SELECT count(*) FROM orders;

insert into orders values(10251,'AROUT',4,'1996-07-08 00:00:00.000','1996-08-05 00:00:00.000','1996-07-15 00:00:00.000',4,41.34,'Victuailles en stock 2','rue du Commerce','Lyon',NULL,'69004','France','empFirstName2');
insert into orders values(10260,'CENTC',13,'1996-07-19 00:00:00.000','1996-08-16 00:00:00.000','1996-07-29 00:00:00.000',13,55.09,'Ottilies Kaoseladen','Mehrheimerstr. 369','Kaqln',NULL,'50739','Germany','empFirstName4');
insert into orders values(10265,'DUMON',18,'1996-07-25 00:00:00.000','1996-08-22 00:00:00.000','1996-08-12 00:00:00.000',18,55.28,'Blondel pare et fils 24', 'place Klacber','Strasbourg',NULL,'67000','France','empFirstName5');

SELECT * FROM orders where FirstName='empFirstName2';
SELECT * FROM orders where FirstName='empFirstName4';
SELECT * FROM orders where FirstName='empFirstName5';

delete from orders where FirstName='empFirstName2';
delete from orders where FirstName='empFirstName4';
delete from orders where FirstName='empFirstName5';

SELECT * FROM orders where FirstName='empFirstName5';

ALTER TABLE order_details ADD CustomerID String;

SELECT count(*) FROM order_details;

insert into order_details values(10250,72,34.8,5,0,'custID4');
insert into order_details values(10249,42,9.8,10,0,'custID14');
insert into order_details values(10253,41,7.7,10,0,'custID7');

SELECT * FROM order_details where CustomerID='custID4';
SELECT * FROM order_details where CustomerID='custID14';
SELECT * FROM order_details where CustomerID='custID7';

delete from order_details where CustomerID='custID4';
delete from order_details where CustomerID='custID14';
delete from order_details where CustomerID='custID7';

SELECT * FROM order_details where CustomerID='custID14';

ALTER TABLE orders DROP COLUMN FirstName;
ALTER TABLE order_details DROP COLUMN CustomerID;

SELECT count(*) FROM orders;
SELECT count(*) FROM order_details;

