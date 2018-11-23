# REVOKE

## Syntax

The syntax used for the REVOKE statement differs depending on whether you revoke privileges for a table or for a routine.

```pre
REVOKE privilege-type ON [ TABLE ] { table-name | view-name } FROM grantees
```

If you do not specify a column list, the statement revokes the privilege for all of the columns in the table.

```pre
REVOKE EXECUTE ON { FUNCTION | PROCEDURE } routine-designator FROM grantees RESTRICT
```

You must use the RESTRICT clause on REVOKE statements for routines. The RESTRICT clause specifies that the EXECUTE privilege cannot be revoked if the specified routine is used in a view or constraint, and the privilege is being revoked from the owner of the view or constraint.

<a id="description"></a>
## Description

The REVOKE statement removes permissions from a specific user or from all users to perform actions on database objects.

The following types of permissions can be revoked:

-   DML operations permissions on a specific table.
-   Insert/Delete data to/from a specific table.
-   Select/Update data permissions on a table or a subset of columns in a table.
-   Create a foreign key reference to the named table or to a subset of columns from a table.
-   Execute permission on a specified routine (function or procedure).

You can revoke privileges from an object if you are the owner of the object or the distributed member owner.

<a id="privilege-type"></a>
## privilege-type

Use the ALL PRIVILEGES privilege type to revoke all of the permissions from the user for the specified table. You can also revoke one or more table privileges by specifying a privilege-list.

Use the DELETE privilege type to revoke permission to delete rows from the specified table.

Use the INSERT privilege type to revoke permission to insert rows into the specified table.

Use the REFERENCES privilege type to revoke permission to create a foreign key reference to the specified table. If a column list is specified with the REFERENCES privilege, the permission is revoked on only the foreign key reference to the specified columns.

Use the SELECT privilege type to revoke permission to perform SELECT statements on a table or view. If a column list is specified with the SELECT privilege, the permission is revoked on only those columns. If no column list is specified, then the privilege is valid on all of the columns in the table.

Use the UPDATE privilege type to revoke permission to use the UPDATE statement on the specified table. If a column list is specified, the permission is revoked only on the specified columns.

<a id="grantees"></a>

## grantees

You can revoke the privileges from specific users or from all users. Use the keyword PUBLIC to specify all users.

The privileges revoked from PUBLIC and from individual users are independent privileges. For example, consider the case where the SELECT privilege on table "t" is granted to both PUBLIC and to the authorization ID "harry." If the SELECT privilege is later revoked from the authorization ID "harry," harry can still access table "t" using the PUBLIC privilege.

!!! Note
	The privileges of the owner (distributed member) of an object cannot be revoked.

<a id="routine-designator"></a>
## routine-designator

```pre
{ qualified-name [ signature ] }
```

<a id="cascading-object-dependencies"></a>
## Cascading Object Dependencies

For views and constraints, if the privilege on which the object depends on is revoked, the object is automatically dropped. SnappyData does not try to determine if you have other privileges that can replace the privileges that are being revoked.

<a id="table-level-privilege-limitations"></a>
## Table-Level Privilege Limitations

All of the table-level privilege types for a specified grantee and table ID are stored in one row in the SYSTABLEPERMS system table. For example, when user2 is granted the SELECT and DELETE privileges on table user1.t1, a row is added to the SYSTABLEPERMS table. The GRANTEE field contains user2 and the TABLEID contains user1.t1. The SELECTPRIV and DELETEPRIV fields are set to Y. The remaining privilege type fields are set to N.

When a grantee creates an object that relies on one of the privilege types, the engine tracks the dependency of the object on the specific row in the SYSTABLEPERMS table. For example, user2 creates the view v1 by using the statement SELECT \* FROM user1.t1, the dependency manager tracks the dependency of view v1 on the row in SYSTABLEPERMS for GRANTEE(user2), TABLEID(user1.t1). The dependency manager knows only that the view is dependent on a privilege type in that specific row, but does not track exactly which privilege type the view is dependent on.

When a REVOKE statement for a table-level privilege is issued for a grantee and table ID, all of the objects that are dependent on the grantee and table ID are dropped. For example, if user1 revokes the DELETE privilege on table t1 from user2, the row in SYSTABLEPERMS for GRANTEE(user2), TABLEID(user1.t1) is modified by the REVOKE statement. The dependency manager sends a revoke invalidation message to the view user2.v1 and the view is dropped even though the view is not dependent on the DELETE privilege for GRANTEE(user2), TABLEID(user1.t1).

<a id="column-Level-privilege-limitations"></a>

## Column-Level Privilege Limitations

Only one type of privilege for a specified grantee and table ID are stored in one row in the SYSCOLPERMS system table. For example, when user2 is granted the SELECT privilege on table user1.t1 for columns c12 and c13, a row is added to the SYSCOLPERMS. The GRANTEE field contains user2, the TABLEID contains user1.t1, the TYPE field contains S, and the COLUMNS field contains c12, c13.

When a grantee creates an object that relies on the privilege type and the subset of columns in a table ID, the engine tracks the dependency of the object on the specific row in the SYSCOLPERMS table. For example, user2 creates the view v1 by using the statement SELECT c11 FROM user1.t1, the dependency manager tracks the dependency of view v1 on the row in SYSCOLPERMS for GRANTEE(user2), TABLEID(user1.t1), TYPE(S). The dependency manager knows that the view is dependent on the SELECT privilege type, but does not track exactly which columns the view is dependent on.

When a REVOKE statement for a column-level privilege is issued for a grantee, table ID, and type, all of the objects that are dependent on the grantee, table ID, and type are dropped. For example, if user1 revokes the SELECT privilege on column c12 on table user1.t1 from user2, the row in SYSCOLPERMS for GRANTEE(user2), TABLEID(user1.t1), TYPE(S) is modified by the REVOKE statement. The dependency manager sends a revoke invalidation message to the view user2.v1 and the view is dropped even though the view is not dependent on the column c12 for GRANTEE(user2), TABLEID(user1.t1), TYPE(S).

## Examples

To revoke the SELECT privilege on table t from the authorization IDs maria and harry:

```pre
REVOKE SELECT ON TABLE t FROM sam,bob;
```

To revoke the UPDATE privileges on table t from the authorization IDs john and smith:

```pre
REVOKE UPDATE ON TABLE t FROM adam,richard;
```

To revoke the SELECT privilege on table s.v from all users:

```pre
REVOKE SELECT ON TABLE test.sample FROM PUBLIC;
```

To revoke the UPDATE privilege on columns c1 and c2 of table s.v from all users:

```pre
REVOKE UPDATE (c1,c2) ON TABLE test.sample FROM PUBLIC;
```

To revoke the EXECUTE privilege on procedure p from the authorization ID george:

```pre
REVOKE EXECUTE ON PROCEDURE p FROM richard RESTRICT;
```


