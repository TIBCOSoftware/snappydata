# Getting Information from SnappyData System Tables

You can monitor many common aspects of SnappyData by using SQL commands (system procedures and simple queries) to collect and analyze data in SnappyData system tables.

## Distributed System Membership Information

The SYS.MEMBERS table provides information about all peers and servers that make up the SnappyData system. You can use different queries to obtain details about individual members and their role in the cluster.

<a id="determine-cluster-membership"></a>
### Determining Cluster Membership

To display a list of all members that participate in a given cluster, simply query all ID entries in sys.members. For example:

```pre
snappy> select ID from SYS.MEMBERS;
ID
-----------------------------------
127.0.0.1(10889)<v1>:43634
127.0.0.1(11045)<v2>:19283
127.0.0.1(10749)<ec><v0>:51430

3 rows selected
```

The number of rows returned corresponds to the total number of peers, servers, and locators in the cluster.

To determine each member's role in the system, include the KIND column in the query:

```pre
snappy> select ID, KIND from SYS.MEMBERS;
ID 					                	 |KIND
-----------------------------------------------------------------------
127.0.0.1(10889)<v1>:43634               |datastore(normal)
127.0.0.1(11045)<v2>:19283               |accessor(normal)
127.0.0.1(10749)<ec><v0>:51430           |locator(normal)

3 rows selected
```
To view the members of cluster, query:

```pre
snappy> show members;
ID                            |HOST     |KIND             |STATUS |THRIFTSERVERS            |SERVERGROUPS
--------------------------------------------------------------------------------------------------------- 
127.0.0.1(10749)<ec><v0>:51430|localhost|locator(normal)  |RUNNING|localhost/127.0.0.1[1527]|
127.0.0.1(10889)<v1>:43634    |localhost|datastore(normal)|RUNNING|localhost/127.0.0.1[1528]|
127.0.0.1(11045)<v2>:19283    |localhost|accessor(normal) |RUNNING|                         |IMPLICIT_LEADER_SERVERGROUP 

3 rows selected
```

Data store members host data in the cluster, while accessor members do not host data. This role is determined by the `host-data` boot property. If a cluster contains only a single data store, its KIND is listed as "loner".

## Table and Data Storage Information

The SYS.SYSTABLES table provides information about all tables that are created in the SnappyData system. You can use different queries to obtain details about tables and the server groups that host data for those tables.

-   [Displaying a List of Tables](#display-list-of-tables)
-   [Determining Whether a Table Is Replicated or Partitioned](#determine-replica-partition)
-   [Determining How Persistent Data Is Stored](#determine-peristent-data)
-   [Displaying Eviction Settings](#display-eviction-setting)
-   [Displaying Indexes](#display-indexes)

<a id="display-list-of-tables"></a>
### Displaying a List of Tables

To display a list of all tables in the cluster:

```pre
snappy> select TABLESCHEMANAME, TABLENAME from SYS.SYSTABLES order by TABLESCHEMANAME;
TABLESCHEMANAME                                    |TABLENAME 
------------------------------------------------------------------------------------------------------------------------
APP                                                |SUPPLIER_1
APP                                                |SUPPLIER
APP                                                |AIRLINEREF
APP                                                |AIRLINE
APP                                                |SNAPPYSYS_INTERNAL____EMPLOYEE_COLUMN_STORE_
APP                                                |EMPLOYEE
SNAPPY_HIVE_METASTORE                              |TBLS
SNAPPY_HIVE_METASTORE                              |PARTITION_PARAMS
SNAPPY_HIVE_METASTORE                              |SKEWED_COL_VALUE_LOC_MAP
SNAPPY_HIVE_METASTORE                              |FUNCS
SNAPPY_HIVE_METASTORE                              |SDS
SNAPPY_HIVE_METASTORE                              |SERDE_PARAMS
SNAPPY_HIVE_METASTORE                              |PART_COL_STATS
SNAPPY_HIVE_METASTORE                              |SKEWED_STRING_LIST
SNAPPY_HIVE_METASTORE                              |DBS
SNAPPY_HIVE_METASTORE                              |PARTITIONS
SNAPPY_HIVE_METASTORE                              |BUCKETING_COLS
SNAPPY_HIVE_METASTORE                              |FUNC_RU
SNAPPY_HIVE_METASTORE                              |SKEWED_VALUES
SNAPPY_HIVE_METASTORE                              |ROLES
SNAPPY_HIVE_METASTORE                              |SORT_COLS
SNAPPY_HIVE_METASTORE                              |SD_PARAMS
SNAPPY_HIVE_METASTORE                              |TAB_COL_STATS
SNAPPY_HIVE_METASTORE                              |GLOBAL_PRIVS
SNAPPY_HIVE_METASTORE                              |SKEWED_COL_NAMES
SNAPPY_HIVE_METASTORE                              |SKEWED_STRING_LIST_VALUES
SNAPPY_HIVE_METASTORE                              |VERSION
SNAPPY_HIVE_METASTORE                              |CDS
SNAPPY_HIVE_METASTORE                              |SEQUENCE_TABLE
SNAPPY_HIVE_METASTORE                              |PARTITION_KEYS
SNAPPY_HIVE_METASTORE                              |TABLE_PARAMS
SNAPPY_HIVE_METASTORE                              |DATABASE_PARAMS
SNAPPY_HIVE_METASTORE                              |COLUMNS_V2
SNAPPY_HIVE_METASTORE                              |SERDES
SNAPPY_HIVE_METASTORE                              |PARTITION_KEY_VALS
SYS                                                |GATEWAYSENDERS
SYS                                                |SYSSTATEMENTS
SYS                                                |SYSKEYS
SYS                                                |SYSROLES
SYS                                                |SYSFILES
SYS                                                |SYSROUTINEPERMS
SYS                                                |SYSCONSTRAINTS
SYS                                                |SYSCOLPERMS
SYS                                                |SYSHDFSSTORES
SYS                                                |SYSDEPENDS
SYS                                                |SYSALIASES
SYS                                                |SYSTABLEPERMS
SYS                                                |SYSTABLES
SYS                                                |SYSVIEWS
SYS                                                |ASYNCEVENTLISTENERS
SYS                                                |SYSCHECKS
SYS                                                |SYSSTATISTICS
SYS                                                |SYSCONGLOMERATES
SYS                                                |GATEWAYRECEIVERS
SYS                                                |SYSTRIGGERS
SYS                                                |SYSDISKSTORES
SYS                                                |SYSSCHEMAS
SYS                                                |SYSFOREIGNKEYS
SYS                                                |SYSCOLUMNS
SYSIBM                                             |SYSDUMMY1
SYSSTAT                                            |SYSXPLAIN_RESULTSETS
SYSSTAT                                            |SYSXPLAIN_STATEMENTS

60 rows selected
```

<a id="determine-replica-partition"></a>

### Determining Whether a Table Is Replicated or Partitioned

The DATAPOLICY column specifies whether a table is replicated or partitioned, and whether a table is persisted to a disk store. For example:

```pre
snappy> select TABLENAME, DATAPOLICY from SYS.SYSTABLES where TABLESCHEMANAME = 'APP';
TABLENAME                                          |DATAPOLICY
--------------------------------------------------------------------------
SUPPLIER_1                                         |PERSISTENT_PARTITION
SUPPLIER                                           |PERSISTENT_PARTITION
AIRLINEREF                                         |PERSISTENT_REPLICATE
AIRLINE                                            |PERSISTENT_REPLICATE
SNAPPYSYS_INTERNAL____EMPLOYEE_COLUMN_STORE_       |PERSISTENT_PARTITION
EMPLOYEE                                           |PERSISTENT_PARTITION

6 rows selected
```

<a id="determine-peristent-data"></a>
### Determining How Persistent Data Is Stored

For persistent tables, you can also display the disk store that persists the table's data, and whether the table uses synchronous or asynchronous persistence:

```pre
snappy> select TABLENAME, DISKATTRS from SYS.SYSTABLES where TABLESCHEMANAME = 'APP';
TABLENAME                                          |DISKATTRS
------------------------------------------------------------------------------------------------------------------------
SUPPLIER_1                                         |DiskStore is GFXD-DEFAULT-DISKSTORE;Asynchronous writes to disk
SUPPLIER                                           |DiskStore is GFXD-DEFAULT-DISKSTORE;Asynchronous writes to disk
AIRLINEREF                                         |DiskStore is GFXD-DEFAULT-DISKSTORE; Synchronous writes to disk
AIRLINE                                            |DiskStore is GFXD-DEFAULT-DISKSTORE; Synchronous writes to disk
SNAPPYSYS_INTERNAL____EMPLOYEE_COLUMN_STORE_       |DiskStore is GFXD-DEFAULT-DISKSTORE; Synchronous writes to disk
EMPLOYEE                                           |DiskStore is SNAPPY-INTERNAL-DELTA; Synchronous writes to disk

6 rows selected
```

<a id="display-eviction-setting"></a>
### Displaying Eviction Settings

Use the EVICTIONATTRS column to determine if a table uses eviction settings and whether a table is configured to overflow to disk. For example:

```pre
snappy> select TABLENAME, EVICTIONATTRS  from SYS.SYSTABLES where TABLESCHEMANAME = 'APP';
TABLENAME                                   |EVICTIONATTRS
---------------------------------------------------------------------------------------------------------------------------
SUPPLIER_1                                  | algorithm=lru-heap-percentage; action=overflow-to-disk; sizer=GfxdObjectSizer
SUPPLIER                                    | algorithm=lru-entry-count; action=overflow-to-disk; maximum=3
AIRLINEREF                                  | algorithm=lru-heap-percentage; action=overflow-to-disk; sizer=GfxdObjectSizer
AIRLINE                                     | algorithm=lru-heap-percentage; action=overflow-to-disk; sizer=GfxdObjectSizer
SNAPPYSYS_INTERNAL____EMPLOYEE_COLUMN_STORE_| algorithm=lru-heap-percentage; action=overflow-to-disk; sizer=GfxdObjectSizer
EMPLOYEE                                    | algorithm=lru-heap-percentage; action=overflow-to-disk; sizer=GfxdObjectSizer

6 rows selected
```
