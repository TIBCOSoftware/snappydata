# UPDATE

Update the value of one or more columns.

##Syntax

``` pre
{ UPDATE table-name [ [ AS ] correlation-name]
        SET column-name = value
        [, column-name = value} ]*
        [ WHERE ]    
}
```

<a id="reference_9518856325F74F79B13674B8E060E6C5__section_F763D37B83E54D828B8572FF3192C67F"></a>
##Description

This form of the UPDATE statement is called a searched update. It updates the value of one or more columns for all rows of the table for which the WHERE clause evaluates to TRUE. Specifying DEFAULT for the update value sets the value of the column to the default defined for that table.

The UPDATE statement returns the number of rows that were updated.

!!! Note:
	Updates on partitioning column and primary key column are not supported. 

<a id="reference_9518856325F74F79B13674B8E060E6C5__section_E3CBEF5040C141AF99A321C394A4C137"></a>

##value

``` pre
expression | DEFAULT
```

<a id="reference_9518856325F74F79B13674B8E060E6C5__section_B9BA421C1A534CADB9FBE16B0B01D5F2"></a>

Statement Dependency System
---------------------------

A searched update statement depends on the table being updated, all of its conglomerates (units of storage such as heaps or indexes), all of its constraints, and any other table named in the WHERE clause or SET expressions.

A CREATE or DROP INDEX statement or an ALTER TABLE statement for the target table of a prepared searched update statement invalidates the prepared searched update statement.

A CREATE or DROP INDEX statement or an ALTER TABLE statement for the target table of a prepared positioned update invalidates the prepared positioned update statement.

Dropping a synonym invalidates a prepared update statement if the latter statement uses the synonym.

Dropping or adding triggers on the target table of the update invalidates the update statement.

Example
-------

``` pre
-- Change the ADDRESS and SINCE  fields to null for all customers with ID greater than 10.
UPDATE TRADE.CUSTOMERS
  SET ADDR=NULL, SINCE=NULL
  WHERE CID > 10;

-- Set the ADDR of all customers to 'VMWare' where the current address is NULL.
UPDATE TRADE.CUSTOMERS
 SET ADDR = 'VMWare'
 WHERE ADDR IS NULL;

// Increase the  QTY field by 10 for all rows of SELLORDERS table.
UPDATE TRADE.SELLORDERS 
 SET QTY = QTY+10;

-- Change the  STATUS  field of  all orders of SELLORDERS table to DEFAULT  ( 'open') , for customer ID = 10
UPDATE TRADE.SELLORDERS
  SET STATUS = DEFAULT
  WHERE CID = 10;
```


