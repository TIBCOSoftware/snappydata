# SET ISOLATION

Change the transaction isolation level for the connection.

##Syntax

``` pre
SET [ CURRENT ] ISOLATION [ = ]
{ 
UR | DIRTY READ | READ UNCOMMITTED 
CS | READ COMMITTED
RS | REPEATABLE READ
RESET
}
```

<a id="reference_10C94598953248B092C202062A1B784B__section_774BCEB643144CFEB13CC9023698A4EA"></a>
##Description

The supported isolation levels in SnappyData are NONE, READ COMMITTED, READ UNCOMMITTED, and REPEATABLE READ. The READ UNCOMMITTED level is implicitly upgraded to READ COMMITTED.

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


