### Move this to the 'best practices' section ...

# DDL Statements in a transaction

SnappyData permits schema and data manipulation statements (DML) within a single transaction. A data definition statement (DDL) is not automatically committed when it is performed, but participates in the transaction within which it is issued.

Although the table itself becomes visible in the system immediately, it acquires exclusive locks on the system tables and the affected tables on all the members in the cluster, so that any DML operations in other transactions will block and wait for the table's locks.

For example, if a new index is created on a table in a transaction, then all other transactions that refer to that table wait for the transaction to commit or roll back. Because of this behavior, as a best practice you should keep transactions that involve DDL statements short (preferably in a single transaction by itself).
