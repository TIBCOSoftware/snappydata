# UPDATE

Update the value of one or more columns.

```pre
{ UPDATE table-name [ [ AS ] correlation-name]
        SET column-name = value
        [, column-name = value} ]*
        [ WHERE ]    
}
```

## Description

This form of the UPDATE statement is called a searched update. It updates the value of one or more columns for all rows of the table for which the WHERE clause evaluates to TRUE. Specifying DEFAULT for the update value sets the value of the column to the default defined for that table.

The UPDATE statement returns the number of rows that were updated.

!!! Note
	- Updates on partitioning column and primary key column is not supported.

    - Delete/Update with a subquery does not work with row tables, if the row table does not have a primary key. An exception to this rule is, if the subquery contains another row table and a simple where clause.

**value**

```pre
expression | DEFAULT
```

## Example

```pre
// Change the ADDRESS and SINCE  fields to null for all customers with ID greater than 10.
UPDATE TRADE.CUSTOMERS SET ADDR=NULL, SINCE=NULL  WHERE CID > 10;

// Set the ADDR of all customers to 'SnappyData' where the current address is NULL.
UPDATE TRADE.CUSTOMERS SET ADDR = 'Snappydata' WHERE ADDR IS NULL;

// Increase the  QTY field by 10 for all rows of SELLORDERS table.
UPDATE TRADE.SELLORDERS SET QTY = QTY+10;

// Change the  STATUS  field of  all orders of SELLORDERS table to DEFAULT  ( 'open') , for customer ID = 10
UPDATE TRADE.SELLORDERS SET STATUS = DEFAULT  WHERE CID = 10;

// Use multiple tables in UPDATE statement with JOIN
UPDATE TRADE.SELLORDERS SET A.TID = B.TID FROM TRADE.SELLORDERS A JOIN TRADE.CUSTOMERS B ON A.CID = B.CID;
```
