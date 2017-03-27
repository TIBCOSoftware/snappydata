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

<a href="../../developers_guide/c_data_consistency.html#concept_8567516F6CA246CEBA352142AAB1F6E9" class="xref" title="All peers in a single distributed system are assumed to be colocated in the same data center and accessible with reliable bandwidth and low latencies. Replication of table data in the distributed system is always eager and synchronous in nature.">Understanding the Data Consistency Model</a> and <a href="../../developers_guide/topics/queries/transactions.html#transactions" class="xref" title="A transaction is a set of one or more SQL statements that make up a logical unit of work that you can commit or roll back, and that will be recovered in the event of a system failure. SnappyData's unique design for distributed transactions allows for linear scaling without compromising atomicity, consistency, isolation, and durability (ACID) properties.">Using Distributed Transactions in Your Applications</a> provide details about non-transactional and transactional behavior in SnappyData.

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


