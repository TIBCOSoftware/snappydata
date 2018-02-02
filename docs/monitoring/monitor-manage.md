# Getting Information from SnappyData System Tables

You can monitor many common aspects of SnappyData by using SQL commands (system procedures and simple queries) to collect and analyze data in SnappyData system tables.

## Distributed System Membership Information

The SYS.MEMBERS table provides information about all peers and servers that make up the SnappyData system. You can use different queries to obtain details about individual members and their role in the cluster.

<a id="determine-cluster-membership"></a>
### Determining Cluster Membership

To display a list of all members that participate in a given cluster, simply query all ID entries in sys.members. For example:

``` pre
snappy> select ID from SYS.MEMBERS;
ID       
------------------------------------------------------------------------------
localhost(10898)<v1>:55213
localhost(11131)<v2>:47059
localhost(10739)<v0>:65055

3 rows selected
```

The number of rows returned corresponds to the total number of peers, servers, and locators in the cluster.

To determine each member's role in the system, include the KIND column in the query:

``` pre
snappy> select ID, KIND from SYS.MEMBERS;
ID       |KIND
------------------------------------------------------------------------------
localhost(10898)<v1>:55213		|datastore(normal)
localhost(11131)<v2>:47059		| accessor(normal)
localhost(10739)<v0>:65055		|locator(normal)

3 rows selected
```

To view the members of cluster, query:

``` pre
snappy> show members;
ID	|HOST	|KIND	|STATUS	|THRIFTSERVERS	|SERVERGROUPS
------------------------------------------------------------------------------------------------------------
localhost(10739)<v0>:65055 |localhost |locator(normal) |RUNNING|localhost/127.0.0.1[1527]|
localhost(10898)<v1>:55213 |localhost |datastore(normal) |RUNNING|localhost/127.0.0.1[1528]|
localhost(11131)<v2>:47059 |localhost |accessor(normal) |RUNNING|IMPLICIT_LEADER_SERVERGROUP|

3 rows selected
```
Data store members host data in the cluster, while accessor members do not host data. This role is determined by the `host-data` boot property. If a cluster contains only a single data store, its KIND is listed as "datastore(loner)."

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

``` bash
select TABLESCHEMANAME, TABLENAME from SYS.SYSTABLES order by TABLESCHEMANAME;
TABLESCHEMANAME        			    |TABLENAME
-----------------------------------------------
APP			                     	|PIZZA_ORDER_PIZZAS
APP			                     	|PIZZA
APP			                     	|TOPPING
APP			                     	|PIZZA_ORDER
APP			                     	|HIBERNATE_SEQUENCES
APP			         				|HOTEL
SNAPPY_HIVE_METASTORE				|TBLS
SNAPPY_HIVE_METASTORE				|PARTITION_PARAMS
SNAPPY_HIVE_METASTORE				|SKEWED_STRING_LIST_VALUES     
SNAPPY_HIVE_METASTORE				|FUNCS
SNAPPY_HIVE_METASTORE				|SDS
SNAPPY_HIVE_METASTORE				|SD_PARAMS
SNAPPY_HIVE_METASTORE				|PART_COL_STATS   
SNAPPY_HIVE_METASTORE				|SKEWED_STRING_LIST
SNAPPY_HIVE_METASTORE				|DBS
SNAPPY_HIVE_METASTORE				|PARTITIONS
SNAPPY_HIVE_METASTORE				|SKEWED_COL_VALUE_LOC_MAP
SNAPPY_HIVE_METASTORE				|FUNC_RU
SNAPPY_HIVE_METASTORE				|SORT_COLS
SNAPPY_HIVE_METASTORE				|ROLES
SNAPPY_HIVE_METASTORE				|BUCKETING_COLS
SNAPPY_HIVE_METASTORE				|SKEWED_COL_NAMES
SNAPPY_HIVE_METASTORE               |TAB_COL_STATS
SNAPPY_HIVE_METASTORE               |GLOBAL_PRIVS
SNAPPY_HIVE_METASTORE               |SKEWED_VALUES
SNAPPY_HIVE_METASTORE               |TABLE_PARAMS
SNAPPY_HIVE_METASTORE               |VERSION
SNAPPY_HIVE_METASTORE               |CDS
SNAPPY_HIVE_METASTORE               |SEQUENCE_TABLE
SNAPPY_HIVE_METASTORE               |PARTITION_KEYS
SNAPPY_HIVE_METASTORE               |COLUMNS_V2
SNAPPY_HIVE_METASTORE               |DATABASE_PARAMS
SNAPPY_HIVE_METASTORE               |SERDE_PARAMS
SNAPPY_HIVE_METASTORE               |SERDES
SNAPPY_HIVE_METASTORE               |PARTITION_KEY_VALS
SYS                                 |GATEWAYSENDERS
SYS                                 |SYSSTATEMENTS
SYS                                 |SYSKEYS
SYS                                 |SYSROLES
SYS                                 |SYSFILES
SYS                                 |SYSROUTINEPERMS
SYS                                 |SYSCONSTRAINTS
SYS                                 |SYSCOLPERMS
SYS                                 |SYSHDFSSTORES
SYS                                 |SYSDEPENDS
SYS                                 |SYSALIASES
SYS                                 |SYSTABLEPERMS
SYS                                 |SYSTABLES
SYS                                 |SYSVIEWS
SYS                                 |ASYNCEVENTLISTENERS
SYS                                 |SYSCHECKS
SYS                                 |SYSSTATISTICS
SYS                                 |SYSCONGLOMERATES
SYS                                 |GATEWAYRECEIVERS
SYS                                 |SYSDISKSTORES
SYS                                 |SYSTRIGGERS
SYS                                 |SYSSCHEMAS
SYS                                 |SYSFOREIGNKEYS
SYS                                 |SYSCOLUMNS
SYSIBM                              |SYSDUMMY1
SYSSTAT                             |SYSXPLAIN_RESULTSETS
SYSSTAT                             |SYSXPLAIN_STATEMENTS

62 rows selected

```

<a id="determine-replica-partition"></a>

### Determining Whether a Table Is Replicated or Partitioned

The DATAPOLICY column specifies whether a table is replicated or partitioned, and whether a table is persisted to a disk store. For example:

``` pre
select TABLENAME, DATAPOLICY from SYS.SYSTABLES where TABLESCHEMANAME = 'APP';
TABLENAME                   |DATAPOLICY
--------------------------------------------------
PIZZA                       |PERSISTENT_REPLICATE
PIZZA_ORDER_PIZZAS          |PARTITION
BASE                        |REPLICATE
TOPPING                     |REPLICATE
PIZZA_TOPPINGS              |PARTITION
PIZZA_ORDER                 |PERSISTENT_PARTITION

6 rows selected
```

<a id="determine-peristent-data"></a>
### Determining How Persistent Data Is Stored

For persistent tables, you can also display the disk store that persists the table's data, and whether the table uses synchronous or asynchronous persistence:

``` pre
select TABLENAME, DISKATTRS from SYS.SYSTABLES where TABLESCHEMANAME = 'APP';
TABLENAME                  |DISKATTRS
----------------------------------------------
PIZZA                      |DiskStore is GFXD-DEFAULT-DISKSTORE; Synchronous writes to disk
PIZZA_ORDER_PIZZAS         |DiskStore is OVERFLOWDISKSTORE;Asynchronous writes to disk
BASE                       |NULL
TOPPING                    |NULL
PIZZA_TOPPINGS             |DiskStore is GFXD-DEFAULT-DISKSTORE; Synchronous writes to disk
PIZZA_ORDER                |DiskStore is GFXD-DEFAULT-DISKSTORE; Synchronous writes to disk

6 rows selected
```

<a id="display-eviction-setting"></a>
### Displaying Eviction Settings

Use the EVICTIONATTRS column to determine if a table uses eviction settings and whether a table is configured to overflow to disk. For example:

``` pre
select TABLENAME, EVICTIONATTRS  from SYS.SYSTABLES where TABLESCHEMANAME = 'APP';
TABLENAME                   |EVICTIONATTRS
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PIZZA                       |NULL
PIZZA_ORDER_PIZZAS          | algorithm=lru-entry-count; action=overflow-to-disk; maximum=100
BASE                        |NULL
TOPPING                     |NULL
PIZZA_TOPPINGS              | algorithm=lru-entry-count; action=overflow-to-disk; maximum=100
PIZZA_ORDER                 |NULL

6 rows selected
```

<a id="display-indexes"></a>
### Displaying Indexes

Join SYSTABLES with CONGLOMERATENAME to determine if a table has an index and display the indexed columns. For example:

``` pre
select CONGLOMERATENAME from SYS.SYSCONGLOMERATES c, SYS.SYSTABLES t 
      where c.ISINDEX = 1 and c.TABLEID = t.TABLEID and t.TABLESCHEMANAME = 'APP' 
      and t.TABLENAME = 'PIZZA';
CONGLOMERATENAME
----------------
2__PIZZA__ID
6__PIZZA__BASE

2 rows selected
```
