# SET ISOLATION

Change the transaction isolation level for the connection.

## Syntax

``` pre
SET [ CURRENT ] ISOLATION [ = ]
{ 
CS | READ COMMITTED
RS | REPEATABLE READ
RESET
}
```

<a id="set-isolation-description"></a>
## Description

The supported isolation levels in SnappyData are NONE, READ COMMITTED and REPEATABLE READ.

Isolation level NONE indicates no transactional behavior. The RESET clause corresponds to the NONE isolation level.

This statement behaves identically to the JDBC *java.sql.Connection.setTransactionIsolation* method and commits the current transaction if isolation level has changed.

Example
-------

``` pre
snappy(PEERCLIENT)> set ISOLATION READ COMMITTED;
0 rows inserted/updated/deleted
snappy(PEERCLIENT)> VALUES CURRENT ISOLATION;
1
----
CS

1 row selected
```


