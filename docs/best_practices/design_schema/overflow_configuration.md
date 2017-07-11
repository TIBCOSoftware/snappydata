<a id="overflow"></a>
## Overflow Configuration

In SnappyData, column tables by default overflow to disk.  For row tables, the use EVICTION_BY clause to evict rows automatically from the in-memory table based on different criteria.  

This allows table data to be either evicted or destroyed based on the current memory consumption of the server. Use the `OVERFLOW` clause to specify the action to be taken upon the eviction event. 

For persistent tables, setting this to 'true' will overflow the table evicted rows to disk based on the EVICTION_BY criteria. Setting this to 'false' will cause the evicted rows to be destroyed in case of eviction event.

Refer to [CREATE TABLE](../reference/sql_reference/create-table.md) link to understand how to configure OVERFLOW and EVICTION_BY clauses.

!!! Note: 
	The default action for OVERFLOW is to destroy the evicted rows.
