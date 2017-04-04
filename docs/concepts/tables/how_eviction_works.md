# How Eviction Works

SnappyData keeps a table's data use under a specified level by removing the least recently used (LRU) data to free memory for new data. You configure table eviction settings based on entry count, the percentage of available heap, or the absolute memory usage of table data. You also configure the action that SnappyData should take to evict data: destroy the data (for partitioned tables only) or overflow data to a disk store, creating a "overflow table."

When SnappyData determines that adding or updating a row would take the table over the specified level, it overflows or removes as many older rows as needed to make room. For entry count eviction, this means a one-to-one trade of an older row for the newer one.

For the memory settings, the number of older rows that need to be removed to make space depends entirely on the relative sizes of the older and newer entries. Eviction controllers monitor the table and memory use and, when the limit is reached, remove older entries to make way for new data. For JVM heap memory percentage, the controller used is the SnappyData resource manager, configured in conjunction with the JVM's garbage collector for optimum performance.

When eviction is enabled, SnappyData manages table rows in a hash map-like structure where the primary key value is used as the key for all other column data in the row. When a row is evicted, the primary key value remains in memory while the remaining column data is evicted.

To configure LRU eviction, see [Create a Table with Eviction Settings](create_table_with_eviction_setting.md) and [CREATE TABLE](../../reference/sql_reference/create-table.md). 
<mark> 
TO BE CONFIRMED 
After you configure eviction features, you can optionally install a RowLoader and/or synchronous writer plug-in to access an external data source, effectively allowing SnappyData to be used as a cache. </mark>
For limitations of this capability, see <a href="../caching_database/eviction_limitations.html#how_eviction_works" class="xref" title="LRU eviction is only effective for operations that operate on a primary key value.">Limitations of Eviction</a>.

