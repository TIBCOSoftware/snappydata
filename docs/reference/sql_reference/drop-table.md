# DROP TABLE

```no-highlight
DROP TABLE [ IF EXISTS ] [schema-name.]table-name
```

## Description

Removes the specified table. Include the `IF EXISTS` clause to execute the statement only if the specified table exists in SnappyData. The *schema-name.* prefix is optional if you are currently using the schema that contains the table.

## Example

```no-highlight
DROP TABLE IF EXISTS app.customer
```
