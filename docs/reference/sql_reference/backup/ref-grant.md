# GRANT

Enable permissions for a specific user or all users.

##Syntax

The syntax for the GRANT statement differs if you are granting privileges to a table or to a routine.

##Syntax for tables

``` pre
GRANT privilege-type ON [ TABLE ] { table-name | view-name } TO grantees
```

##Syntax for routines

``` pre
GRANT EXECUTE ON { FUNCTION | PROCEDURE } routine-designator TO grantees
```

<a id="reference_0A0EF7C16FA64666ACF1A0EF92135505__section_F307D69A482D402E969B449D79340DB9"></a>
##Description

The GRANT statement enables permissions for a specific user or all users to perform actions on SQL objects.

The following types of permissions can be granted:

-   Perform DML operations on a specific table.
-   Insert/Delete rows from a table.
-   Select/Update data on a table or subset of columns in a table.
-   Create a foreign key reference to the named table or to a subset of columns from a table.
-   Define trigger on a table.
-   Run a specified function or procedure.

!!!Note:
	This release of SnappyData does not support Loader/Writer/Listener privileges, Data Aware Procedure privileges, where clause read privileges, or Write Behind Listeners privileges.

You cannot grant privileges to gateway senders or gateway senders when using WAN replication.

A GRANT statement is honored only when authorization mode is enabled by setting the `snappydata.sql-authorization` property to true.

Privileges authorized to a user can only be granted by the user to others. See the CREATE statement for the respective SQL object that you want to grant privileges on for more information.

<a id="reference_0A0EF7C16FA64666ACF1A0EF92135505__section_1BF770D59A3A4677AA7464283DD654CE"></a>

##privilege-type

``` pre
ALL PRIVILEGES |  privilege-list
```

Use the ALL PRIVILEGES privilege type to grant all of the permissions to the user for the specified table. You can also grant one or more table privileges by specifying a privilege-list.

<a id="reference_0A0EF7C16FA64666ACF1A0EF92135505__section_9CF62FAE46184654A49554F3C3430CDA"></a>

##privilege-list

``` pre
table-privilege {, table-privilege }*
```

<a id="reference_0A0EF7C16FA64666ACF1A0EF92135505__section_8AE1BD89C90B42A494B92CC2A0F8918C"></a>

##table-privilege

``` pre
ALTER | DELETE | INSERT | REFERENCES [column-list] | SELECT [column-list] |
TRIGGER | UPDATE [ column-list ]
```

Use the ALTER privilege to grant permission to the <a href="ref-alter-table.html#reference_12F6A629A0BD46DCBDB0175D9EA946F2" class="xref noPageCitation" title="Use the ALTER TABLE statement to add columns and constraints to an existing table, remove them from a table, or modify other table features such as AsyncEventListener implementations and gateway sender configurations.">ALTER TABLE</a> command on the specified table.

Use the DELETE privilege type to grant permission to delete rows from the specified table.

Use the INSERT privilege type to grant permission to insert rows into the specified table.

Use the REFERENCES privilege type to grant permission to create a foreign key reference to the specified table. If a column list is specified with the REFERENCES privilege, the permission is valid on only the foreign key reference to the specified columns.

Use the SELECT privilege type to grant permission to perform SELECT statements on a table or view. If a column list is specified with the SELECT privilege, the permission is valid on only those columns. If no column list is specified, then the privilege is valid on all of the columns in the table.

Use the TRIGGER privilege type to grant permission to create a trigger on the specified table.

Use the UPDATE privilege type to grant permission to use the UPDATE statement on the specified table. If a column list is specified, the permission applies only to the specified columns. To update a row using a statement that includes a WHERE clause, you must have SELECT permission on the columns in the row that you want to update.

<a id="reference_0A0EF7C16FA64666ACF1A0EF92135505__section_83340AC8B6B442EA95F1C1D958E2D928"></a>

##column-list

``` pre
( column-identifier {, column-identifier }* )
```

<a id="reference_0A0EF7C16FA64666ACF1A0EF92135505__section_84099B885CE84330A3CF8142420DA8FE"></a>

##grantees

``` pre
{ authorization ID | PUBLIC } [,{ authorization ID | PUBLIC } ] *
```

You can grant privileges for specific users or for all users. Use the keyword PUBLIC to specify all users. When PUBLIC is specified, the privileges affect all current and future users. The privileges granted to PUBLIC and to individual users are independent privileges. For example, a SELECT privilege on table t is granted to both PUBLIC and to the authorization ID harry. The SELECT privilege is later revoked from the authorization ID harry, but Harry can access the table t through the PUBLIC privilege.

<a id="reference_0A0EF7C16FA64666ACF1A0EF92135505__section_FAD351E130AD4020B01A1F215A0352D1"></a>

##routine-designator

``` pre
{ function-name | procedure-name }
```

##Example

To grant the SELECT privilege on table "t" to the authorization IDs "sam" and "bob:"

``` pre
GRANT SELECT ON TABLE t TO sam,bob;
```

To grant the UPDATE and TRIGGER privileges on table "t" to the authorization IDs "sagarika" and "czhu:"

``` pre
GRANT UPDATE, TRIGGER ON TABLE t TO sagarika,czhu;
```

To grant ALTER TABLE privileges on table "t" to the authorization ID "elek:"

``` pre
GRANT ALTER ON TABLE t TO elek;
```

To grant the SELECT privilege on table "test.sample" to all users:

``` pre
GRANT SELECT ON TABLE test.sample to PUBLIC;
```

To grant the EXECUTE privilege on procedure"p" to the authorization ID "richard:"

``` pre
GRANT EXECUTE ON PROCEDURE p TO richard;
```


