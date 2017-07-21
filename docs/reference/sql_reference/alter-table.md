# ALTER TABLE

Use the ALTER TABLE statement to add and drop columns in row tables using SnappyData APIs or SQL.

!!! Note: 

	* This is not supported on column tables.

	* For row tables, only adding and dropping a column is supported using Snappy APIs or SQL.

## Syntax

```
ALTER TABLE table-name
{
  ADD COLUMN column-definition |
  DROP [ COLUMN ] column-name [ RESTRICT ] |
}

```

## Example
```
-- create a table with no constraints
CREATE TABLE trade.customers (
    cid int not null,
    cust_name varchar(100),
    addr varchar(100),
    tid int);

-- drop a non-primary key column if the column is not used for table partitioning, and the column has no dependents
ALTER TABLE trade.customers drop column addr;

-- add the column back with a default value
ALTER TABLE trade.customers add column addr varchar(100);
```
