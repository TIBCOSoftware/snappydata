# DELETE

Delete rows from a table.

```pre
{
DELETE FROM table-name [ ]
[ WHERE ]
}
```

## Description

This form is called a searched delete, removes all rows identified by the table name and WHERE clause.

## Example

```pre
// Delete rows from the CUSTOMERS table where the CID is equal to 10.
DELETE FROM TRADE.CUSTOMERS WHERE CID = 10;

// Delete all rows from table T.
DELETE FROM T;
```

**Related Topics**</br>

* [CREATE TABLE](create-table.md)

* [DROP TABLE](drop-table.md)

* [SHOW TABLES](../interactive_commands/show.md#tables)

* [TRUNCATE TABLE](truncate-table.md)
