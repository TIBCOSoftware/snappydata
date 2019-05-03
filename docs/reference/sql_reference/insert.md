# INSERT

An INSERT statement creates a row or rows and stores them in the named table. The number of values assigned in an INSERT statement must be the same as the number of specified or implied columns.

```pre
INSERT INTO table-name
    [ ( simple-column-name [ , simple-column-name ]* ) ]
   Query
```

## Description

The query can be:

-   a VALUES list
-   a multiple-row VALUES expression

!!! Note
	TIBCO ComputeDB does not support an INSERT with a subselect query if any subselect query requires aggregation. </p>
    
Single-row and multiple-row lists can include the keyword DEFAULT. Specifying DEFAULT for a column inserts the column's default value into the column. Another way to insert the default value into the column is to omit the column from the column list and only insert values into other columns in the table.

For more information, refer to [SELECT](select.md).

## Example

```pre

--create table trade.customers
CREATE TABLE TRADE.CUSTOMERS (CID INT, CUST_NAME VARCHAR(100), SINCE DATE, ADDR VARCHAR(100));

INSERT INTO TRADE.CUSTOMERS VALUES (1, 'User 1', '2001-10-12', 'SnappyData');

-- Insert a new customer into the CUSTOMERS  table,
-- but do not assign  value to  'SINCE'  column
INSERT INTO TRADE.CUSTOMERS(CID, CUST_NAME,  ADDR) VALUES (2, 'USER 2', 'SnappyData');

-- Insert two new customers using one statement 
-- into the CUSTOMER table as in the previous example, 
-- but do not assign  value to 'SINCE'  field of the new customer.
INSERT INTO TRADE.CUSTOMERS (CID ,CUST_NAME , ADDR) VALUES (3, 'User 3' , 'SnappyData'), (4, 'User 4' , 'SnappyData');

-- Insert the DEFAULT value for the SINCE column
INSERT INTO TRADE.CUSTOMERS VALUES (1, 'User 1', DEFAULT, 'SnappyData');

-- Insert into another table using a select statement.
INSERT INTO TRADE.NEWCUSTOMERS SELECT * from TRADE.CUSTOMERS WHERE CUST_NAME='User 1';
```


