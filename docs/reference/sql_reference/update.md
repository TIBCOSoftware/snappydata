# UPDATE

Update the value of one or more columns.

```pre
UPDATE table-name
SET column-name = value
[, column-name = value ]*
[ WHERE predicate ]
value: 
expression | DEFAULT
```

## Description

This form of the UPDATE statement is called a searched update. It updates the value of one or more columns for all rows of the table for which the WHERE clause evaluates to TRUE. Specifying DEFAULT for the update value sets the value of the column to the default defined for that table.

The UPDATE statement returns the number of rows that were updated.

!!! Note
	- Updates on partitioning columns and primary key columns are not supported.

    - Delete/Update with a subquery does not work with row tables, if the row table does not have a primary key. An exception to this rule is, if the subquery contains another row table and a simple where clause.

Implicit conversion of string to numeric value is not performed in an UPDATE statement when the string type expression is used as part of a binary arithmetic expression.

For example, the following SQL fails with AnalysisException. Here `age` is an `int` type column:

```
update users set age = age + '2'
```

As a workaround, you can apply an explicit cast, if the string is a number.

For example:

```
update users set age = age + cast ('2' as int)
```

!!!Note
	It is important to note that when you cast a non-numeric string, it results in a `NULL` value which is reflected in the table as a result of the update. This behavior of casting is inherited from Apache Spark, as Spark performs a fail-safe casting. For example, the following statement populates `age` column with `NULL` values: `update users set age = age + cast ('abc' as int)`

Assigning a non-matching type expression to a column also fails with `AnalysisException`. However, the assignment is allowed in the following cases, even if the data type does not match:

- assigning null value
- assigning narrower decimal to a wider decimal
- assigning narrower numeric type to wider numeric type as long as precision is
not compromised
- assigning of narrower numeric types to decimal type
- assigning expression of any data type to a string type column

For example, the following statement fails with `AnalysisException`:

```
update users set age = '45'
```

Here also, the workaround is to cast the expression explicitly. For example, the following statement will pass:

```
update users set age = cast ('45' as int)
```

## Example

```pre
// Change the ADDRESS and SINCE  fields to null for all customers with ID greater than 10.
UPDATE TRADE.CUSTOMERS SET ADDR=NULL, SINCE=NULL  WHERE CID > 10;

// Set the ADDR of all customers to 'ComputeDB' where the current address is NULL.
UPDATE TRADE.CUSTOMERS SET ADDR = 'ComputeDB' WHERE ADDR IS NULL;

// Increase the  QTY field by 10 for all rows of SELLORDERS table.
UPDATE TRADE.SELLORDERS SET QTY = QTY+10;

// Change the  STATUS  field of  all orders of SELLORDERS table to DEFAULT  ( 'open') , for customer ID = 10
UPDATE TRADE.SELLORDERS SET STATUS = DEFAULT  WHERE CID = 10;

// Use multiple tables in UPDATE statement with JOIN
UPDATE TRADE.SELLORDERS SET A.TID = B.TID FROM TRADE.SELLORDERS A JOIN TRADE.CUSTOMERS B ON A.CID = B.CID;
```
