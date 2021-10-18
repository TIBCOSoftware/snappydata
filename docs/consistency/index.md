# Overview of SnappyData Distributed Transactions

SnappyData supports transaction characteristics of isolation and atomicity. Transactions are supported using JDBC/ODBC through statements such as SET [autocommit](../reference/interactive_commands/autocommit.md), [SET Isolation](../reference/sql_reference/set-isolation.md), [COMMIT](../reference/interactive_commands/commit.md), and [ROLLBACK](../reference/interactive_commands/rollback.md).  


!!! Note
    - Full distributed transactions (that is, multiple update SQL statements in one logical transaction) is currently supported only for row tables.

    - Column tables only support single statement implicit transactions. That is, every DML (insert/update/delete) statement is executed in a implicit transaction. The DML statement can in-fact be a multi-row statement and is executed with "all or nothing" semantics.

    - Transactions execution do not depend on a central locking facility and is highly scalable.

    - SnappyData supports high concurrency for transactions. Readers (queries) do not acquire locks and isolated from concurrent transactions using an MVCC implementation.

    - Currently, demarcated transactions (Commit, rollback) is only supported through the JDBC and ODBC API. Support for commit/rollback will be added to the Spark API will be added in a later release.

**Additional Information**

* [How to use Transactions Isolation Levels](../howto/use_transactions_isolation_levels.md)

* [Best Practices for using Distributed Transactions and Snapshot Isolation](../best_practices/transactions_best_practices.md)

