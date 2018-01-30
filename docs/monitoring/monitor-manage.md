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
To view the members of cluster, queryl:

``` pre
snappy> show members;
ID	|HOST	|KIND	|STATUS	|THRIFTSERVERS	|SERVERGROUPS
------------------------------------------------------------------------------------------------------------------------
localhost(10739)<v0>:65055    |localhost	|locator(normal)	|RUNNING	|localhost/127.0.0.1[1527]     |                              
localhost(10898)<v1>:55213    |localhost	|datastore(normal)	|RUNNING	|localhost/127.0.0.1[1528]     |                              
localhost(11131)<v2>:47059    |localhost	|accessor(normal)	|RUNNING	|         	|IMPLICIT_LEADER_SERVERGROUP

3 rows selected
```
Data store members host data in the cluster, while accessor members do not host data. This role is determined by the `host-data` boot property. If a cluster contains only a single data store, its KIND is listed as "datastore(loner)."

## Table and Data Storage Information

The SYS.SYSTABLES table provides information about all tables that are created in the SnappyData system. You can use different queries to obtain details about tables and the server groups that host data for those tables.

-   [Displaying a List of Tables](#display-list-of-tables)
-   [Determining Where Data Is Stored](#determine-data-stored)
-   [Determining Whether a Table Is Replicated or Partitioned](#determine-replica-partition)
-   [Determining How Persistent Data Is Stored](#determine-peristent-data)
-   [Displaying Eviction Settings](#display-eviction-setting)
-   [Displaying Indexes](#display-indexes)
-   [Displaying Installed AsyncEventListeners](#display_asynceventlisterners)

<a id="display-list-of-tables"></a>
### Displaying a List of Tables

To display a list of all tables in the cluster:

``` pre
select TABLESCHEMANAME, TABLENAME from SYS.SYSTABLES order by TABLESCHEMANAME;
TABLESCHEMANAME            |TABLENAME
-----------------------------------------------
APP                        |PIZZA_ORDER_PIZZAS
APP                        |PIZZA
APP                        |TOPPING
APP                        |PIZZA_ORDER
APP                        |HIBERNATE_SEQUENCES
APP                        |HOTEL
APP                        |BASE
APP                        |PIZZA_TOPPINGS
APP                        |BOOKING
APP                        |CUSTOMER
SYS                        |SYSCONSTRAINTS                                                                                                                  
SYS                        |ASYNCEVENTLISTENERS                                                                                                             
SYS                        |SYSCOLPERMS                                                                                                                     
SYS                        |SYSKEYS                                                                                                                         
SYS                        |SYSFILES                                                                                                                        
SYS                        |SYSCONGLOMERATES                                                                                                                
SYS                        |SYSDEPENDS                                                                                                                      
SYS                        |SYSROUTINEPERMS                                                                                                                 
SYS                        |SYSTRIGGERS                                                                                                                     
SYS                        |SYSTABLES                                                                                                                       
SYS                        |SYSALIASES                                                                                                                      
SYS                        |SYSROLES                                                                                                                        
SYS                        |SYSDISKSTORES                                                                                                                   
SYS                        |SYSVIEWS                                                                                                                        
SYS                        |SYSSTATISTICS                                                                                                                   
SYS                        |SYSCOLUMNS                                                                                                                      
SYS                        |SYSCHECKS                                                                                                                       
SYS                        |GATEWAYSENDERS                                                                                                                  
SYS                        |SYSFOREIGNKEYS                                                                                                                  
SYS                        |SYSSCHEMAS                                                                                                                      
SYS                        |SYSTABLEPERMS                                                                                                                   
SYS                        |SYSSTATEMENTS                                                                                                                   
SYSIBM                     |SYSDUMMY1                                                                                                                       
SYSSTAT                    |SYSXPLAIN_SORT_PROPS                                                                                                            
SYSSTAT                    |SYSXPLAIN_DIST_PROPS                                                                                                            
SYSSTAT                    |SYSXPLAIN_STATEMENTS                                                                                                            
SYSSTAT                    |SYSXPLAIN_RESULTSET_TIMINGS                                                                                                     
SYSSTAT                    |SYSXPLAIN_SCAN_PROPS                                                                                                            
SYSSTAT                    |SYSXPLAIN_STATEMENT_TIMINGS                                                                                                     
SYSSTAT                    |SYSXPLAIN_RESULTSETS                                                                                                            

40 rows selected
```

<a id="determine-data-stored"></a>

### Determining Where Data Is Stored


To determine which tables are deployed to a specific set of server groups:

``` pre
select TABLESCHEMANAME, TABLENAME from SYS.SYSTABLES 
       where GROUPSINTERSECT(SERVERGROUPS, 'SG1,SG2');
TABLESCHEMANAME          |TABLENAME
--------------------------------------------------------------
APP                      |PIZZA_ORDER_PIZZAS
APP                      |PIZZA_ORDER
APP                      |BASE
APP                      |PIZZA_TOPPINGS

4 rows selected
```

For a specific table or set of tables, you can list all of the SnappyData members that host that table's data:

``` pre
select m.ID from SYS.SYSTABLES t, SYS.MEMBERS m where t.TABLESCHEMANAME='APP' 
     and t.TABLENAME='PIZZA' and  m.HOSTDATA = 1 
     and (LENGTH(t.SERVERGROUPS) = 0 or GROUPSINTERSECT(t.SERVERGROUPS, m.SERVERGROUPS));
ID
-------------------------------------
vmc-ssrc-rh156(23870)<v1>:23802/60824
vmc-ssrc-rh154(26751)<v4>:42054/49195
vmc-ssrc-rh156(23897)<v2>:37163/43747
vmc-ssrc-rh154(26739)<v3>:9287/48842

4 rows selected
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

<a id="display_asynceventlisterners"></a>
### Displaying Installed AsyncEventListeners

If you install AsyncEventListener implementations, you can join the SYSTABLES, MEMBERS, and ASYNCEVENTLISTENERS tables to display the listener implementations associated with a table as well as the data store ID on which the listener is installed:

``` pre
select t.*, m.ID DSID from SYS.SYSTABLES t, SYS.MEMBERS m, SYS.ASYNCEVENTLISTENERS a
       where t.tablename='<table>' and groupsintersect(a.SERVER_GROUPS, m.SERVERGROUPS)
       and groupsintersect(t.ASYNCLISTENERS, a.ID);
```

See <a href="../../../reference/system_tables/rrefsistabs24269.html#rrefsistabs24269" class="xref" title="Describes the tables and views in the distributed system.">SYSTABLES</a> and <a href="../../../reference/system_tables/asynceventlisteners_table.html#reference_0EC88800DBDF4D18B8EB91EA537ABF6B" class="xref" title="Describes the configuration of AsyncEventListener implementations.">ASYNCEVENTLISTENERS</a>.

