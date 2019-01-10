# DROP TABLE/EXTERNAL TABLE/SAMPLE TABLE

```pre
DROP TABLE [ IF EXISTS ] [schema-name.]table-name
```

## Description

Removes the specified table. Include the `IF EXISTS` clause to execute the statement only if the specified table exists in SnappyData. The *schema-name.* prefix is optional if you are currently using the schema that contains the table.

## Example

```pre
DROP TABLE IF EXISTS app.customer
```

**Related Topics**</br>

* [CREATE TABLE](create-table.md)

* [CREATE EXTERNAL TABLE](create-external-table.md)

* [CREATE SAMPLE TABLE](create-sample-table.md)

* [DELETE TABLE](delete.md)

* [SHOW TABLES](../interactive_commands/show.md#tables)

* [TRUNCATE TABLE](truncate-table.md)
