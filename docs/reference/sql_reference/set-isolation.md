# SET ISOLATION

Change the transaction isolation level for the connection.

## Syntax

```pre
SET [ CURRENT ] ISOLATION [ = ]
{ 
CS | READ COMMITTED
RS | REPEATABLE READ
RESET
}
```

<a id="set-isolation-description"></a>
## Description

The supported isolation levels in TIBCO ComputeDB are NONE, READ COMMITTED and REPEATABLE READ.

Isolation level NONE indicates no transactional behavior. This is a special constant that indicates that transactions are not supported. </br>
The RESET clause corresponds to the NONE isolation level. For more information, see [Overview of TIBCO ComputeDB Distributed Transactions](../../consistency/using_transactions_row.md).

This statement behaves identically to the JDBC *java.sql.Connection.setTransactionIsolation* method and commits the current transaction if isolation level has changed.

Example
-------

```pre
snappy> set ISOLATION READ COMMITTED;

snappy> VALUES CURRENT ISOLATION;
1
----
CS

1 row selected
```
