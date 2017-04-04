# Limitations of Eviction
LRU eviction is only effective for operations that operate on a primary key value.

Consider these limitations before you implement LRU eviction in your system:

* Eviction with the DESTROY action is not supported for replicated tables. (Eviction with the OVERFLOW action is supported for replicated tables, as is the EXPIRE Clause).

* SnappyData does not support transactions on tables that are configured with the DESTROY evict action. This restriction exists because the requirements of ACID transactions can conflict with the semantics of destroying evicted entries. For example, a transaction may need to update a number of entries that is greater than the amount allowed by the eviction setting. Transactions are supported with the OVERFLOW evict action, because the required entries can be loaded into memory as necessary to support transaction semantics.

* The capability to synchronize with an external data source is only effective for select/update/delete operations that query data by primary key. Accessing data by other criteria may result in incomplete results, because a RowLoader is only invoked on primary key queries.

* If you configure a partitioned table with the DESTROY eviction action, you must ensure that all queries against the table filter results using a primary key value. Queries that do not filter on a primary key may yield partial results if rows are destroyed on eviction. This limitation does not apply to tables that are configured with the OVERFLOW eviction action.

* You cannot create a foreign key reference to a partitioned table that is configured for eviction or expiration with the DESTROY action. This limitation does not apply to tables that are configured with the OVERFLOW eviction action. <mark>To be Confirmed </br>See EVICTION BY Clause for more information. </mark>

* Some applications may benefit from evicting rows to disk in order to reduce memory usage (when in heap or off-heap memory). However, enabling eviction increases the per-row overhead on heap memory required by SnappyData to perform LRU eviction for the table. As a general rule, table eviction is only helpful for conserving memory if the non-primary key columns in a table are large: 100 bytes or more.

* An UPDATE will not occur on a row that has been evicted or has expired from the cache with the DESTROY action. This limitation does not apply to tables that are configured with the OVERFLOW eviction action.

* An INSERT will succeed if an identical row (based on primary key) has been previously evicted or expired from the cache with the DESTROY action, but the row still exists in the external data store.