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

**Example** </br>
```pre
snappy> select * from SYS.SYSTABLES;
TABLEID                             |TABLENAME                |TABLETYPE|SCHEMAID                            |TABLESCHEMANAME      |LOCKGRANULARITY|SERVERGROUPS|DATAPOLICY          |PARTITIONATTRS|RESOLVER|EXPIRATIONATTRS|EVICTIONATTRS|DISKATTRS                                                 |LOADER|WRITER|LISTENERS|ASYNCLISTENERS|GATEWAYENABLED|GATEWAYSENDERS|OFFHEAPENABLED
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
df2180fd-0162-84af-a660-0000f2ef3af8|TBLS                     |T        |d46b80cd-0162-84af-a660-0000f2ef3af8|SNAPPY_HIVE_METASTORE|R              |            |PERSISTENT_REPLICATE|NULL          |NULL    |NULL           |NULL         |DiskStore is GFXD-DD-DISKSTORE; Synchronous writes to disk|NULL  |NULL  |NULL     |NULL          |false         |NULL          |false         
0073400e-00b6-fdfc-71ce-000b0a763800|GATEWAYSENDERS           |S        |8000000d-00d0-fd77-3ed8-000a0a0b1900|SYS                  |R              |            |NORMAL              |NULL          |NULL    |NULL           |NULL         |NULL                                                      |NULL  |NULL  |NULL     |NULL          |false         |NULL          |false         
80000000-00d1-15f7-ab70-000a0a0b1500|SYSSTATEMENTS            |S        |8000000d-00d0-fd77-3ed8-000a0a0b1900|SYS                  |R              |            |NORMAL              |NULL          |NULL    |NULL           |NULL         |NULL                                                      |NULL  |NULL  |NULL     |NULL          |false         |NULL          |false         
a6c0c1fe-0162-84af-a660-0000f2ef3af8|PARTITION_KEY_VALS       |T        |d46b80cd-0162-84af-a660-0000f2ef3af8|SNAPPY_HIVE_METASTORE|R              |            |PERSISTENT_REPLICATE|NULL          |NULL    |NULL           |NULL         |DiskStore is GFXD-DD-DISKSTORE; Synchronous writes to disk|NULL  |NULL  |NULL     |NULL          |false         |NULL          |false         
140d4147-0162-84af-a660-0000f2ef3af8|SORT_COLS                |T        |d46b80cd-0162-84af-a660-0000f2ef3af8|SNAPPY_HIVE_METASTORE|R              |            |PERSISTENT_REPLICATE|NULL          |NULL    |NULL           |NULL         |DiskStore is GFXD-DD-DISKSTORE; Synchronous writes to disk|NULL  |NULL  |NULL     |NULL          |false         |NULL          |false         
9fc7c266-0162-84af-a660-0000f2ef3af8|FUNCS                    |T        |d46b80cd-0162-84af-a660-0000f2ef3af8|SNAPPY_HIVE_METASTORE|R              |            |PERSISTENT_REPLICATE|NULL          |NULL    |NULL           |NULL         |DiskStore is GFXD-DD-DISKSTORE; Synchronous writes to disk|NULL  |NULL  |NULL     |NULL          |false         |NULL          |false         
f33d40c7-0162-84af-a660-0000f2ef3af8|SYSXPLAIN_RESULTSETS     |C        |c013800d-00fb-2644-07ec-000000134f30|SYSSTAT              |R              |            |NORMAL              |NULL          |NULL    |NULL           |NULL         |NULL                                                      |NULL  |NULL  |NULL     |NULL          |false         |NULL          |false         
80000039-00d0-fd77-3ed8-000a0a0b1900|SYSKEYS                  |S        |8000000d-00d0-fd77-3ed8-000a0a0b1900|SYS                  |R              |            |NORMAL              |NULL          |NULL    |NULL           |NULL         |NULL                                                      |NULL  |NULL  |NULL     |NULL          |false         |NULL          |false         
e03f4017-0115-382c-08df-ffffe275b270|SYSROLES                 |S        |8000000d-00d0-fd77-3ed8-000a0a0b1900|SYS                  |R              |            |NORMAL              |NULL          |NULL    |NULL           |NULL         |NULL                                                      |NULL  |NULL  |NULL     |NULL          |false         |NULL          |false         
80000000-00d3-e222-873f-000a0a0b1900|SYSFILES                 |S        |8000000d-00d0-fd77-3ed8-000a0a0b1900|SYS                  |R              |            |NORMAL              |NULL          |NULL    |NULL           |NULL         |NULL                                                      |NULL  |NULL  |NULL     |NULL          |false         |NULL          |false         
2057c01b-0103-0e39-b8e7-00000010f010|SYSROUTINEPERMS          |S        |8000000d-00d0-fd77-3ed8-000a0a0b1900|SYS                  |R              |            |NORMAL              |NULL          |NULL    |NULL           |NULL         |NULL                                                      |NULL  |NULL  |NULL     |NULL          |false         |NULL          |false         
65548115-0162-84af-a660-0000f2ef3af8|SDS                      |T        |d46b80cd-0162-84af-a660-0000f2ef3af8|SNAPPY_HIVE_METASTORE|R              |            |PERSISTENT_REPLICATE|NULL          |NULL    |NULL           |NULL         |DiskStore is GFXD-DD-DISKSTORE; Synchronous writes to disk|NULL  |NULL  |NULL     |NULL          |false         |NULL          |false         
96a2414f-0162-84af-a660-0000f2ef3af8|SD_PARAMS                |T        |d46b80cd-0162-84af-a660-0000f2ef3af8|SNAPPY_HIVE_METASTORE|R              |            |PERSISTENT_REPLICATE|NULL          |NULL    |NULL           |NULL         |DiskStore is GFXD-DD-DISKSTORE; Synchronous writes to disk|NULL  |NULL  |NULL     |NULL          |false         |NULL          |false         
```
