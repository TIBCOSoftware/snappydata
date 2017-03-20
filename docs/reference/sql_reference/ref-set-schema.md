# SET SCHEMA

Set or change the default schema for a connection's session.

##Syntax

``` pre
SET [ CURRENT ] SCHEMA [ = ]
{ schema-name |
USER | ? | 'string-constant' }
```

<a id="reference_6CBB1645D7E74B0096C9F556AC754EBE__section_F731B973A8A6465DA0065687ABB5FA84"></a>
##Description

The SET SCHEMA statement sets or changes the default schema for a connection's session to the provided schema. This is then used as the schema for all statements issued from the connection that do not explicitly specify a schema name. The SET SCHEMA statement is not transactional and does not affect commit or rollback in any manner.

The USER clause above denotes the current user and causes the current schema to be set to the name of the current user, else if no current user is defined then it defaults to the inbuilt APP schema.

? is a dynamic parameter specification that can be used in prepared statements that can be executed with different schema values after a prepare.

##Example

``` pre
-- below are equivalent assuming a TRADE schema
SET SCHEMA TRADE;
SET SCHEMA trade;
SET CURRENT SCHEMA = trade;
SET CURRENT SCHEMA "TRADE"; –- quoted identifier
–- lower case won't be found
SET SCHEMA = 'trade';

-- this sets the default schema to the current user id 
SET CURRENT SCHEMA USER;
```


