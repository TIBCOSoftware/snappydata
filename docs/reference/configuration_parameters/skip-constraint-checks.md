# skip-constraint-checks

## Description

When `skip-constraint-checks` is set to true, SnappyData ignores all primary key, foreign key, and unique constraints for all SQL statements that are executed over the connection. This connection property is typically used only when importing data into a SnappyData system, in order to speed the execution of insert statements. 

!!! Note
	When you set this property on a connection, you must ensure that no SQL operations violate the foreign key and unique constraints defined in the system. For primary key constraints, SnappyData uses the [PUT INTO](../sql_reference/put-into.md#reference_2A553C72CF7346D890FC904D8654E062) DML syntax to ensure that only the last primary key value inserted or updated remains in the system; this preserves primary key constraints. However, foreign key and unique constraints *can* be violated when this property is set. This can lead to undefined behavior in the system, because other connections that do not enable `skip-constraint-checks` still require constraint checks for correct operation.</p>

One exception to the `skip-constraint-checks` behavior is that SnappyData will throw a constraint violation error if a SQL statement would cause a local unique index to have duplicate values. This type of local index is created when you specify a unique index on a replicated table, or on partitioned tables where the number unique index columns is greater than or equal to the table's partitioning columns. The exception does not apply to updating global indexes, because SnappyData uses the [PUT INTO](../sql_reference/put-into.md#reference_2A553C72CF7346D890FC904D8654E062) DML syntax to update global indexes when `skip-constraint-checks` is enabled. Using [PUT INTO](../sql_reference/put-into.md#reference_2A553C72CF7346D890FC904D8654E062) ensures that only the last index entry for a given value remains in the index, which preserves uniqueness.

## Default Value

false

## Property Type

connection

## Prefix

n/a
