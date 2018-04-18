# SET SCHEMA

Set or change the default schema for a connection's session.

```no-highlight
SET SCHEMA schema-name
```

## Description

The SET SCHEMA statement sets or changes the default schema for a connection's session to the provided schema. This is then used as the schema for all statements issued from the connection that does not explicitly specify a schema name. </br>
The default schema is APP.

## Example

```no-highlight
-- below are equivalent assuming a TRADE schema
SET SCHEMA TRADE;
SET SCHEMA trade;
```


