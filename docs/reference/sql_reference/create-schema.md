# CREATE SCHEMA

Creates a schema with the given name which provides a mechanism to logically group objects.

##Syntax

``` pre
CREATE SCHEMA schema-name;
```

<a id="create-schema__section_0944689B84B945FBB8259BA9C0C2977F"></a>
##Description

This creates a schema with the given name which provides a mechanism to logically group objects by providing a namespace for objects. This can then be used by other CREATE statements as the namespace prefix. For example, CREATE TABLE SCHEMA1.TABLE1 ( ... ) will create a table TABLE1 in the schema SCHEMA1. The DEFAULT SERVER GROUPS for a schema specifies the server groups used by the CREATE TABLE statement by default when no explicit server groups have been mentioned.

<!--SECURITY RELATED INFO (WHEN IMPLEMENTED)
The CREATE SCHEMA statement is subject to access control when the <a href="../configuration/ConnectionAttributes.html#jdbc_connection_attributes__section_98DEF23ED88A4821BF4CA852CBB5633A" class="xref noPageCitation">snappydata.sql-authorization</a> property is set to true for the system. Only the system user can create a schema with a name different from the current user name, and only the system user can specify `AUTHORIZATION user-name` with a *user-name* other than the current user name.

There is no single owner of the entire distributed system. Instead, ownership is defined by the distributed member joining the system. The distributed member process must boot up using theuser attribute in the properties to indicate owner of that process. A member that boots in this way can create a schema or grant access to a schema across the distributed system.
-->
##Example

``` pre
CREATE SCHEMA myschema;
```
<!-- SECURITY RELATED INFO (WHEN IMPLEMENTED)
–- create schema that uses the authorization id 'shared' as schema-name
CREATE SCHEMA AUTHORIZATION shared;

-- create schema flights and authorize anita to all the objects that use the schema.
CREATE SCHEMA flights AUTHORIZATION anita;

-->


