# ALTER TABLE

Use the ALTER TABLE statement to add and drop columns in row tables using SnappyData API or SQL.

!!! Note

	* ALTER TABLE is not supported on column, temporary and external tables.

	* For row tables, only adding and dropping a column is supported using Snappy APIs or SQL.

## Syntax

**SQL**
```pre
ALTER TABLE table-name
{
  ADD COLUMN column-definition |
  DROP COLUMN column-name
}
```

**API:**
```pre
snc.alterTable(tableName, isAddColumn, column)
```

## Example

**SQL:**

```pre
-- create a table
CREATE TABLE trade.customers (
    cid int not null,
    cust_name varchar(100),
    addr varchar(100),
    tid int);

-- drop a non-primary key column if the column is not used for table partitioning, and the column has no dependents
ALTER TABLE trade.customers DROP COLUMN addr;

-- add the column back with a default value
ALTER TABLE trade.customers ADD COLUMN addr varchar(100);
```

**API:**

```pre
//create a table in Snappy store
	snc.createTable("orders", "row", ordersDF.schema, Map.empty[String, String])

//alter table add/drop specified column, only supported for row tables.

// for adding a column isAddColumn should be true
	snc.alterTable("orders", true, StructField("FirstName", StringType, true))

// for dropping a column isAddColumn should be false
	snc.alterTable("orders", false, StructField("FirstName", StringType, true))
```
