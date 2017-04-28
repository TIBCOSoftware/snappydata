# Create a Table with Eviction Settings
Use eviction settings to keep your table within a specified limit, either by removing evicted data completely or by creating an overflow table that persists the evicted data to a disk store.

**Procedure**

Follow these steps to configure table eviction settings. Refer to CREATE TABLE for details about specifying eviction settings.

1. Decide whether to evict based on:

	* Entry count (useful if table row sizes are relatively uniform).

	* Total bytes used.

	* Percentage of JVM heap used. This uses the SnappyData resource manager. When the manager determines that eviction is required, the manager orders the eviction controller to start evicting from all tables where the eviction criterion is set to 'LRUHEAPPERCENT'.
	You can configure a global heap eviction percentage for all SnappyData data stores, or configure threshold percentage for one or more server groups by using system procedures or by specifying `-eviction-heap-percentage` when starting up your server using `snappy`. [Procedures](../../../reference/inbuilt/SystemProcedures.md) describes how to configure the heap eviction percentage using system procedures. See [server](../../../reference/snappy_shell_reference/store-server.md) for information on using gfxd to start your servers.
	Eviction continues until the resource manager calls a halt. SnappyData evicts the least recently used rows hosted by the member for the table.
    
2. Decide what action to take when the limit is reached:

	* Locally destroy the row (partitioned tables only).
    
	!!!Note
		SnappyData does not propagate the DESTROY evict action to configured callback implementations, such as DBSynchronizer. Do not configure eviction with the DESTROY action on a table that has dependent tables (for example, child rows with foreign keys). If a DELETE statement is called for a parent table row that was locally destroyed through eviction, the DELETE succeeds in SnappyData. However, the DELETE operation can later fail in the backend database when DBSynchronizer asynchronously sends the DELETE command, if dependent rows still exist.

	If eviction with the DESTROY action is required for dependent tables, consider using a trigger or writer implementation to listen for DELETE events on the parent table. The trigger or writer should fail the DELETE operation if child rows are found to exist in the backend database.
	The DESTROY eviction action is not supported for replicated tables.

## Overflow the row data to disk

!!!Note
   	When you configure an overflow table, only the evicted rows are written to disk. If you restart or shut down a member that hosts the overflow table, the table data that was in memory is not restored unless you explicitly configure persistence (or you configure one or more replicas with a partitioned table). See [Persisting Table Data to SnappyData Disk Stores]. 
        
1. If you want to overflow data to disk (or persist the entire table to disk), configure a named disk store to use for the overflow data. If you do not specify a disk store when creating an overflow table, SnappyData stores the overflow data in the default disk store.

2. Create the table with the required eviction configuration.
	For example, to evict using LRU entry count and overflow evicted rows to a disk store:
    
	```
    CREATE TABLE Orders(OrderId INT NOT NULL,ItemId INT ) 
    EVICTION BY LRUCOUNT 2 EVICTACTION OVERFLOW 'OverflowDiskStore' ASYNCHRONOUS
    ```
    
	If you do not specify a disk store, SnappyData overflows table data to the default disk store.
	To create an overflow table and persist the entire table, so that the entire table is available through peer restarts:

	```
		snappy> CREATE TABLE Orders(OrderId INT NOT NULL,ItemId INT ) 
		> EVICTION BY LRUCOUNT 2 EVICTACTION OVERFLOW PERSISTENT 'OverflowDiskStore' ASYNCHRONOUS;
	```
    
	The table uses the same named disk store for both overflow and persistence.
	To create a table that simply removes evicted data from memory without persisting the evicted data, use the DESTROY eviction action. For example:

	```
    	snappy> CREATE TABLE Orders(OrderId INT NOT NULL,ItemId INT ) 
		> PARTITION BY COLUMN (OrderId) EVICTION BY LRUMEMSIZE 1000 EVICTACTION DESTROY;
	```

	!!! Note
		If you configure LRUMEMSIZE for a partitioned table, SnappyData recommends that you set both LRUMEMSIZE and MAXPARTSIZE to the same value. If you specify an LRUMEMSIZE value that is larger than a partitioned table’s MAXPARTSIZE, then SnappyData automatically sets LRUMEMSIZE equal to MAXPARTSIZE. See EVICTION BY Clause.
