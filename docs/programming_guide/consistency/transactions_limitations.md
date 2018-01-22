# Transaction Functionality and Limitations

!!!Hint:
	Distributed transaction is supported only for row tables.
    
In this release of SnappyData, the scope for transactional functionality is:

-   The result set that is obtained from executing a query should either be completely consumed, or the result set is explicitly closed. Otherwise, DDL operations wait until the ResultSet is garbage-collected.
-   Transactions for persistent tables are enabled by default, but the full range of fault tolerance is not yet implemented. It is assumed that at least one copy of a row is always available (redundant members are available) in the event of member failures.
-   SQL statements that implicitly place locks, such as `select for update`, are not supported outside of transactions (default isolation level).
-   The supported isolation levels are 'READ COMMITTED' and 'READ UNCOMMITTED' where both behave as 'READ COMMITTED.' Autocommit is OFF by default in SnappyData, unlike in other JDBC drivers.
-   Transactions always do "write-write" conflict detection at operation or commit time. Applications do not need to use `select for update` or explicit locking to get this behavior, as compared to other databases. (`select for update` is not supported outside of a transaction.)
-   Nested transactions and savepoints are not supported.
-   SnappyData does not support transactions on partitioned tables that are configured with the DESTROY evict action. This restriction exists because the requirements of ACID transactions can conflict with the semantics of destroying evicted entries. For example, a transaction may need to update a number of entries that is greater than the amount allowed by the eviction setting. Transactions are supported with the OVERFLOW evict action, because the required entries can be loaded into memory as necessary to support transaction semantics.
-   SnappyData does not restrict concurrent non-transactional clients from updating tables that may be involved in transactions. This is by design, to maintain very high performance when no transactions are in use. If an application uses transactions on a table, make sure the application consistently uses transactions when updating that table.
-   All DML on a single row is atomic in nature inside or outside of transactions.
-   There is a small window during a commit when the committed set is being applied to the underlying table and concurrent readers, which do not consult any transactional state, have visibility to the partially-committed state. The larger the transaction, the larger the window. Also, transaction state is maintained in a memory-based buffer. The shorter and smaller the transaction, the less likely the transaction manager will run short on memory.
