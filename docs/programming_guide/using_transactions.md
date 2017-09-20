# Using Transactions

## Using Transaction Isolation levels
Transactions specify an [isolation level](../reference/sql_reference/set-isolation.md) that defines the degree to which one transaction must be isolated from resource or data modifications made by other transactions. The transaction isolation levels define the type of locks acquired on read operations. Only one of the isolation level options can be set at a time, and it remains set for that connection until it is explicitly changed.

!!! Note:
	* If you set the isolation level to `READ_COMMITTED` or `REPEATABLE_READ`, queries on column table report an error if [autocommit](../reference/interactive_commands/autocommit.md) is set to off (false). </br>Queries on column tables are supported when isolation level is set to `READ_COMMITTED` or `REPEATABLE_READ` and autocommit is set to **true**.

    * DDL execution (for example [CREATE TABLE](../reference/sql_reference/create-table.md) /[DROP TABLE](../reference/sql_reference/drop-table.md)) is not allowed when `autocommit` is set to `false`  and transaction isolation level is `READ_COMMITTED` or `REPEATABLE_READ`.  DDL commands reports syntax error in such cases. DDL execution is allowed if `autocommit` is `true` for `READ_COMMITTED` or `REPEATABLE_READ` isolation levels.


The following isolation levels are supported for row tables:

| Isolation level | Description |
|--------|--------|
|NONE|Default isolation level. The Database Engine uses shared locks to prevent other transactions from modifying rows while the current transaction is running a read operation. |
|READ_COMMITTED|SnappyData ensures that ongoing transactional as well as non-transactional (isolation-level NONE) operations never read uncommitted (dirty) data. SnappyData accomplishes this by maintaining transactional changes in a separate transaction state that are applied to the actual data-store for the table only at commit time. SnappyData detects only Write-Write conflicts while in READ_COMMITTED isolation level. </br>In READ COMMITTED, a read view is created at the start of each statement and lasts only as long as each statement execution.|
|REPEATABLE_READ|In this isolation level, a lock-based concurrency control DBMS implementation keeps read and write locks (acquired on selected data) until the end of the transaction. In REPEATABLE READ every lock acquired during a transaction is held for the duration of the transaction.|

For more information, see, [SET ISOLATION](../reference/sql_reference/set-isolation.md)

## Using Snapshot Isolation for Column Tables

Multi-Statement transactions are not supported on column tables. Instead, we provide snapshot isolation by default.  Snapshot ensures that all queries see the same version (snapshot), of the database, based on the state of the database at the moment in time when the query is executed. The snapshot is taken per statement for each partition, which means, the snapshot of the partition is taken the moment the query accesses the partition. This behavior is set by default for column tables and cannot be modified.

By default, all individual operations (read/write) on column table have snapshot isolation with `autocommit` set to `ON`. This means, in case of a failure the user operation fails and [rollback](../reference/interactive_commands/rollback.md) is triggered. </br>
You cannot set [autocommit](../reference/interactive_commands/autocommit.md) to `Off`. Snapshot isolation ensures that changes made, after the ongoing operation has taken a snapshot is not visible partially or totally.</br>
If there are concurrent updates in a row, then the last commit is used.

!!! Note:
	To get per statement transactional behavior, all the write operations should span only one partition.

	However, if you have operations that span multiple partitions, then, ensure that:

	* In case of failure on one partition, the operation is retried on another copy of the same partition. Set [redundancy](../reference/sql_reference/create-table.md) to more than 0, if transactional behavior with operations spanning more than one partition is required.

	* If the operation fails on all the redundant copies of a partition and the same operation succeeds on some of the other partitions, then, partial rollback is initiated.</br>
	In this case, you can retry the operation at the application level.

<!--Currently, only single statement snapshot isolation (that is, [autocommit](../reference/interactive_commands/autocommit.md) must be set to true) is supported.-->  
