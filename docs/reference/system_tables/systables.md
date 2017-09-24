# SYSTABLES

Describes the tables and views in the distributed system.

|Column Name|Type|Length|Nullable|Contents|
|---|---|---|---|----|
|TABLEID|CHAR|36|No|Unique identifier for table or view|
|TABLENAME|VARCHAR|128|No|Table or view name|
|TABLETYPE|CHAR|1|No|'S' (system table), 'T' (user table), 'A' (synonym), or 'V' (view)|
|SCHEMAID|CHAR|36|No|Schema ID for the table or view|
|TABLESCHEMANAME|VARCHAR|128|No|The table schema|
|LOCKGRANULARITY|CHAR|1|No|Lock granularity for the table: 'T' (table level locking) or 'R' (row level locking, the default)|
|SERVERGROUPS|VARCHAR|128|No|The server groups assigned to the table|
|DATAPOLICY|VARCHAR|24|No|Table partitioning and replication status|
|PARTITIONATTRS|LONG VARCHAR|32,700|Yes|For partitioned tables, displays the additional partitioning attributes assigned with the CREATE TABLE statement, such as colocation, buckets, and redundancy values|
|RESOLVER|LONG VARCHAR|32,700|Yes|The partitioning resolver (contains the partitioning clause).|
|EXPIRATIONATTRS|LONG VARCHAR|32,700|Yes|Row expiration settings|
|EVICTIONATTRS|LONG VARCHAR|32,700|Yes|Row eviction settings|
|DISKATTRS|LONG VARCHAR|32,700|Yes|Table persistence settings|
|LOADER|VARCHAR|128|Yes|Not available for this release|
|WRITER|VARCHAR|128|Yes|Not available for this release|
|LISTENERS|LONG VARCHAR|32,700|Yes|Not available for this release|
|ASYNCLISTENERS|VARCHAR|256|Yes|Not available for this release|
|GATEWAYENABLED|BOOLEAN|1|No|Not available for this release|
|GATEWAYSENDERS|VARCHAR|256|Yes|Not available for this release|


