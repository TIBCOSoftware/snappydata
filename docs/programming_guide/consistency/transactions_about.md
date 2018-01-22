# Overview of SnappyData Distributed Transactions

!!!Hint:
	Distributed transaction is supported only for row tables.

All statements in a transaction are atomic. A transaction is associated with a single connection (and database) and cannot span connections. In addition to providing linear scaling, the SnappyData transaction design minimizes messaging requirements, so that short-lived transactions are efficient.

## How the Transaction Model Works

When data is managed in partitioned tables, each row is implicitly owned by a single member for non-transactional operations. However, with distributed transactions, all copies of a row are treated as being equivalent, and updates are routed from a single "owning" member to all copies in parallel. This makes the transactional behavior for partitioned tables similar to the behavior for replicated tables. The transaction manager works closely with the SnappyData membership management system to make sure that, irrespective of failures or adding/removing members, changes to all rows are either applied to all available copies at commit time, or they are applied to none.

!!! Note:
	SnappyData does not support adding new members to a cluster for an ongoing transaction. If you add a new member to the cluster in the middle of a transaction and the new member is to store data involved in the transaction, SnappyData implicitly rolls back the transaction and throws a SQLException (SQLState: "X0Z05").

There is no centralized transaction coordinator in SnappyData. Instead, the member on which a transaction was started acts as the coordinator for the duration of the transaction. If the application updates one or more rows, the transaction coordinator determines which owning members are involved, and acquires local "write" locks on all of the copies of the rows. At commit time, all changes are applied to the local cache and any redundant copies. If another concurrent transaction attempts to change one of the rows, the local "write" acquisition fails for the row, and that transaction is automatically rolled back. In the case where there is no persistent table involved, there is no need to issue a two-phase commit to redundant members; in this case, commits are efficient, single-phase operations.

Unlike traditional distributed databases, SnappyData does not use write-ahead logging for transaction recovery in case the commit fails during replication or redundant updates to one or more members. The most likely failure scenario is one where the member is unhealthy and gets forced out of the distributed system, guaranteeing the consistency of the data. When the failed member comes back online, it automatically recovers the replicated/redundant data set and establishes coherency with the other members. If all copies of some data go down before the commit is issued, then this condition is detected using the group membership system, and the transaction is rolled back automatically on all members.

The following images represent the functioning of read and write operations in the transaction model:

![Read Operations](../../Images/transactions_read.png)

![Write Operations](../../Images/transactions_write.png)
