# Overview of SnappyData Distributed Transactions

SnappyData supports transaction characteristics of isolation and atomicity. Transactions are supported using JDBC/ODBC through statements such as SET autocommit, SET Isolation, COMMIT, and ROLLBACK. Refer to the [SQL reference guide](...) for the syntax. 


Note the following:
- Full distributed transactions (i.e. multiple update SQL statements in one logical transaction) is only currently supported over Row tables. 
- Column tables only support single statement implicit transactions. i.e. Every DML(insert/update/delete) statement is executed in a implicit transaction. The DML statement could in-fact be a multi-row statement and will executed with "all or nothing" semantics. 
- Transactions execution do not depend on a central locking facility and is highly scalable. 
- Snappydata supports high concurrency for transactions. Readers (queries) do not acquire locks and isolated from concurrent transactions using a MVCC implementation. 
- Currently, demarcated transactions (Commit, rollback) is only supported through the JDBC and ODBC API. Support for commit/rollback will be added to the Spark API will be added in a later release.

## How Transactions Works

There is no centralized transaction coordinator in SnappyData. Instead, the member on which a transaction was started acts as the coordinator for the duration of the transaction. If the application updates one or more rows, the transaction coordinator determines which owning members are involved, and acquires local "write" locks on all of the copies of the rows. At commit time, all changes are applied to the local store and any redundant copies. If another concurrent transaction attempts to change one of the rows, the local "write" acquisition fails for the row, and that transaction is automatically rolled back.

Unlike traditional distributed databases, SnappyData does not use write-ahead logging for transaction recovery in case the commit fails during replication or redundant updates to one or more members. The most likely failure scenario is one where the member is unhealthy and gets forced out of the distributed system, guaranteeing the consistency of the data. When the failed member comes back online, it automatically recovers the replicated/redundant data set and establishes coherency with the other members. If all copies of some data go down before the commit is issued, then this condition is detected using the group membership system, and the transaction is rolled back automatically on all members.

!!! Note:
	SnappyData does not support transactions while new data store members are added while in progress. If you add a new member to the cluster in the middle of a transaction and the new member is involved in the transaction (e.g. owns a partition of the data or is a replica), SnappyData implicitly rolls back the transaction and throws a SQLException (SQLState: "X0Z05").


The following images represent the functioning of read and write operations in the transaction model:

![Read Operations](../../Images/transactions_read.png)

![Write Operations](../../Images/transactions_write.png)
