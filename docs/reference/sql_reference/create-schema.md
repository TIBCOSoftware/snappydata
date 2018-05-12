# CREATE SCHEMA

```no-highlight
CREATE SCHEMA schema-name;
```

!!! Note:
	Schema names with trailing underscores are not supported.

## Description

This creates a schema which provides a mechanism to logically group objects by providing a namespace for objects. This can then be used by other CREATE statements as the namespace prefix.

## Example

```no-highlight
CREATE SCHEMA myschema;
```

To create a table TABLE1 in the schema SCHEMA1:
```
CREATE TABLE SCHEMA1.TABLE1 ( ... )
```

