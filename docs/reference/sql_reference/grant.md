# GRANT

## Syntax

The syntax for the GRANT statement differs if you are granting privileges to a table or to a routine.

## Syntax for Tables

```pre
GRANT privilege-type ON [ TABLE ] { table-name | view-name } TO grantees
```

## Syntax for Routines

```pre
GRANT EXECUTE ON { FUNCTION | PROCEDURE } routine-designator TO grantees
```

## Description

The GRANT statement enables permissions for a specific user or all users to perform actions on SQL objects.

The following types of permissions can be granted:

-   Perform DML operations on a specific table.
-   Insert/Delete rows from a table.
-   Select/Update data on a table or subset of columns in a table.
-   Create a foreign key reference to the named table or to a subset of columns from a table.
-   Run a specified function or procedure.

<a id="privilege-type"></a>
## privilege-type

```pre
ALL PRIVILEGES |  privilege-list
```

Use the ALL PRIVILEGES privilege type to grant all of the permissions to the user for the specified table. You can also grant one or more table privileges by specifying a privilege-list.

<a id="privilege-list"></a>

## privilege-list

```pre
table-privilege {, table-privilege }*
```

<a id="table-privilege"></a>

## table-privilege

```pre
ALTER | DELETE | INSERT | REFERENCES [column-list] | SELECT [column-list] |
 UPDATE [ column-list ]
```

Use the [ALTER](alter-table.md) privilege to grant permission to the command on the specified table.

Use the [DELETE](delete.md) privilege type to grant permission to delete rows from the specified table.

Use the [INSERT](insert.md) privilege type to grant permission to insert rows into the specified table.

<!--Use the REFERENCES privilege type to grant permission to create a foreign key reference to the specified table. If a column list is specified with the REFERENCES privilege, the permission is valid on only the foreign key reference to the specified columns.-->

Use the [SELECT](select.md) privilege type to grant permission to perform SELECT statements on a table or view. If a column list is specified with the SELECT privilege, the permission is valid on only those columns. If no column list is specified, then the privilege is valid on all of the columns in the table.


Use the [UPDATE](update.md) privilege type to grant permission to use the UPDATE statement on the specified table. If a column list is specified, the permission applies only to the specified columns. To update a row using a statement that includes a WHERE clause, you must have [SELECT](select.md) permission on the columns in the row that you want to update.

<a id="column-list"></a>

## column-list

```pre
( column-identifier {, column-identifier }* )
```

<a id="grantees"></a>

## grantees

```pre
{ authorization ID | PUBLIC } [,{ authorization ID | PUBLIC } ] *
```

You can grant privileges for specific users or for all users. Use the keyword PUBLIC to specify all users. When PUBLIC is specified, the privileges affect all current and future users. The privileges granted to PUBLIC and to individual users are independent privileges. For example, a SELECT privilege on table t is granted to both PUBLIC and to the authorization ID harry. The SELECT privilege is later revoked from the authorization ID harry, but Harry can access the table t through the PUBLIC privilege.

<a id="routine-designator"></a>

## routine-designator

```pre
{ function-name | procedure-name }
```

## Example

To grant the SELECT privilege on table "t" to the authorization IDs "sam" and "bob:"

```pre
GRANT SELECT ON TABLE t TO sam,bob;
```

To grant the UPDATE privileges on table "t" to the authorization IDs "john" and "smith:"

```pre
GRANT UPDATE ON TABLE t TO john,smith;
```

To grant ALTER TABLE privileges on table "t" to the authorization ID "adam:"

```pre
GRANT ALTER ON TABLE t TO adam;
```

To grant the SELECT privilege on table "test.sample" to all users:

```pre
GRANT SELECT ON TABLE test.sample to PUBLIC;
```

<!--To grant the EXECUTE privilege on procedure"p" to the authorization ID "richard:"

```pre
GRANT EXECUTE ON PROCEDURE p TO richard;
``` 
-->
