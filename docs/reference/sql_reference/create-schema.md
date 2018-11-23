# CREATE SCHEMA


```pre
CREATE SCHEMA schema-name;
```


## Description

This creates a schema with the given name which provides a mechanism to logically group objects by providing a namespace for objects. This can then be used by other CREATE statements as the namespace prefix. For example, CREATE TABLE SCHEMA1.TABLE1 ( ... ) will create a table TABLE1 in the schema SCHEMA1. 

!!! Note
	Schema names with trailing underscores are not supported.

The CREATE SCHEMA statement is subject to access control when the **gemfirexd.sql-authorization ** property is set to true for the system. Only the system user can create a schema with a name different from the current user name, and only the system user can specify AUTHORIZATION user-name with a user-name other than the current user name.


## Example

*	Create schema

```pre
CREATE SCHEMA myschema;
```

*	Create schema that uses the authorization id '**shared**' as schema-name

```pre
CREATE SCHEMA AUTHORIZATION shared;
```

*	Create schema **flights** and authorize **anita** to all the objects that use the schema.

```pre
CREATE SCHEMA flights AUTHORIZATION anita;
```
*	Create schema **reports** and authorize all members of LDAP group **finance** to all the objects that use the schema. Any member of this LDAP group can GRANT or REVOKE permissions on objects in this schema to other users.

```pre
CREATE SCHEMA reports AUTHORIZATION ldapgroup:finance;
```
