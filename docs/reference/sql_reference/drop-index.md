# DROP INDEX

```pre
DROP INDEX [ IF EXISTS ] [schema-name.]index-name;
```

## Description

Drops the index in the given schema (or current schema if none is provided). Include the `IF EXISTS` clause to execute the statement only if the specified index exists in SnappyData.

## Example

```pre
DROP INDEX IF EXISTS app.idx;
```

