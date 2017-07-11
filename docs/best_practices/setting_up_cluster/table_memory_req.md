# Table Memory Requirements

SnappyData column tables encodes data for compression and hence require memory that is less than or equal to the on-disk size of the uncompressed data. If the memory-size is configured (i.e. off-heap is enabled), the entire column table is stored in off-heap memory. 

SnappyData row tables memory requirements have to be calculated by taking into account row overheads. Row tables have different amounts of heap memory overhead per table and index entry, which depends on whether you persist table data or configure tables for overflow to disk.

| TABLE IS PERSISTED?	 | OVERFLOW IS CONFIGURED?	 |APPROXIMATE HEAP OVERHEAD |
|--------|--------|--------|
|No|No|64 bytes|
|Yes|No|120 bytes|
|Yes|Yes|152 bytes|

!!! Note
	For a persistent, partitioned row table, SnappyData uses an additional 16 bytes per entry used to improve the speed of recovering data from disk. When an entry is deleted, a tombstone entry of approximately 13 bytes is created and maintained until the tombstone expires or is garbage-collected in the member that hosts the table. (When an entry is destroyed, the member temporarily retains the entry to detect possible conflicts with operations that have occurred. This retained entry is referred to as a tombstone.)
    
    
| TYPE OF INDEX ENTRY | APPROXIMATE HEAP OVERHEAD |
|--------|--------|
|New index entry     |80 bytes|
|First non-unique index entry|24 bytes|
|Subsequent non-unique index entry|8 bytes to 24 bytes*|

_If there are more than 100 entries for a single index entry, the heap overhead per entry increases from 8 bytes to approximately 24 bytes._