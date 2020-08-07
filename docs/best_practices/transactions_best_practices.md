# TIBCO ComputeDB Distributed Transactions

<a id="snapshot-bestpractise"></a>
## Using Transactions

-   For high performance, minimize the duration of transactions to avoid conflicts with other concurrent transactions. If atomicity for only single row updates is required, then completely avoid using transactions because TIBCO ComputeDB provides atomicity and isolation for single rows without transactions.

-   When using transactions, keep the number of rows involved in the transaction as low as possible. TIBCO ComputeDB acquires locks eagerly, and long-lasting transactions increase the probability of conflicts and transaction failures. Avoid transactions for large batch update statements or statements that affect a lot of rows. 

-   Unlike in traditional databases, TIBCO ComputeDB transactions can fail with a conflict exception on writes instead of on commit. This choice makes sense given that the outcome of the transaction has been determined to fail.

-   To the extent possible, model your database so that most transactions operate on colocated data. When all transactional data is on a single member, then stricter isolation guarantees are provided.


- DDL Statements in a transaction
    TIBCO ComputeDB permits schema and data manipulation statements (DML) within a single transaction. A data definition statement (DDL) is not automatically committed when it is performed but participates in the transaction within which it is issued.

    Although the table itself becomes visible in the system immediately, it acquires exclusive locks on the system tables and the affected tables on all the members in the cluster, so that any DML operations in other transactions will block and wait for the table's locks.

    For example, if a new index is created on a table in a transaction, then all other transactions that refer to that table wait for the transaction to commit or rollback. Because of this behavior, as a best practice, you should keep transactions that involve DDL statements short (preferably in a single transaction by itself).
    
<a id="snapshot-bestpractise"></a>
## Using Snapshot Isolation

To the extent possible, model your database so that most transactions operate on colocated data. When all transactional data is on a single member, then stricter isolation guarantees are provided. In case of failure, the rollback is complete and not partial.

**More information**

- [Overview of TIBCO ComputeDB Distributed Transactions](../consistency/transactions_about.md)

- [How to use Transactions Isolation Levels](../howto/use_transactions_isolation_levels.md)
