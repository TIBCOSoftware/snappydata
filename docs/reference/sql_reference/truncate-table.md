# TRUNCATE TABLE

Remove all content from a table and return it to its initial, empty state. TRUNCATE TABLE clears all in-memory data for the specified table as well as any data that was persisted to SnappyData disk stores. 

```pre
TRUNCATE TABLE table-name
```

## Description

To truncate a table, you must be the table's owner. You cannot use this command to truncate system tables.

## Example

To truncate the "flights" table in the current schema:

```pre
TRUNCATE TABLE flights;
```

**Related Topics**</br>

* [CREATE TABLE](create-table.md)

* [DROP TABLE](drop-table.md)

* [DELETE TABLE](delete.md)

* [SHOW TABLES](../interactive_commands/show.md#tables)
