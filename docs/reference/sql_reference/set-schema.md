# SET SCHEMA

Set or change the default schema for a connection's session.

##Syntax

``` pre
	SET SCHEMA schema-name
```

##Description

The SET SCHEMA statement sets or changes the default schema for a connection's session to the provided schema. This is then used as the schema for all statements issued from the connection that do not explicitly specify a schema name. 

The default schema is APP.


##Example

``` pre
-- below are equivalent assuming a TRADE schema
SET SCHEMA TRADE;
SET SCHEMA trade;
```


