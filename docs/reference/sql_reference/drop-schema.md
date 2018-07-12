# DROP SCHEMA

```pre
DROP TABLE [schema-name] restrict;
```

## Description

Permanently removes a schema from the database. Ensure that you delete all the objects that exist in a schema before you drop it. This is an irreversible process.

## Example

```pre
drop schema trade restrict;
```
