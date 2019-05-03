# SHOW

Displays information about active connections and database objects.

## Syntax

```pre
SHOW
{
   CONNECTIONS |
   FUNCTIONS   |
   IMPORTEDKEYS [ IN schemaName | FROM table-Name ] |
   INDEXES [ IN schemaName | FROM table-Name ] |
   PROCEDURES [ IN schemaName ] |
   SCHEMAS |
   TABLES [ IN schemaName ] |
   VIEWS [ IN schemaName ] |
}
```
The following are covered in this section:

- [SHOW CONNECTIONS](#connections)

- [SHOW FUNCTIONS](#functions)

- [SHOW IMPORTEDKEYS](#importedkeys)

- [SHOW INDEXES](#indexes)

- [SHOW PROCEDURES](#procedures)

- [SHOW SCHEMAS](#schemas)

- [SHOW TABLES](#tables)

- [SHOW VIEWS](#views)

## Description

Displays information about active connections and database objects.

<a id= connections> </a>
<h4>**SHOW CONNECTIONS**</h4>

If there are no connections, the SHOW CONNECTIONS command returns "No connections available".

Otherwise, the command displays a list of connection names and the URLs used to connect to them. The currently active connection is marked with an \* after its name.

**Example**

```pre
snappy> show connections;
CONNECTION0* - 	jdbc:snappydata:thrift://127.0.0.1[1527]
* = current connection
```

<a id= functions> </a>
<h4>**SHOW FUNCTIONS**</h4>

Displays the details of the default system functions.

Currently, UDF functions are not displayed in the list. This will be available in the future releases.

**Example**

```pre
snappy> show functions;
FUNCTION_SCHEM      |FUNCTION_NAME                 |REMARKS             
------------------------------------------------------------------------ 
SNAPPY_HIVE_METASTO&|NUCLEUS_ASCII                 |org.datanucleus.sto&
SNAPPY_HIVE_METASTO&|NUCLEUS_MATCHES               |org.datanucleus.sto&
SYS                 |CHECK_TABLE_EX                |com.pivotal.gemfire&
SYS                 |GET_CRITICAL_HEAP_PERCENTAGE  |com.pivotal.gemfire&
SYS                 |GET_CRITICAL_OFFHEAP_PERCENTA&|com.pivotal.gemfire&
SYS                 |GET_EVICTION_HEAP_PERCENTAGE  |com.pivotal.gemfire&
SYS                 |GET_EVICTION_OFFHEAP_PERCENTA&|com.pivotal.gemfire&
SYS                 |GET_IS_NATIVE_NANOTIMER       |com.pivotal.gemfire&
SYS                 |GET_NATIVE_NANOTIMER_TYPE     |com.pivotal.gemfire&
SYS                 |GET_SNAPSHOT_TXID_AND_HOSTURL |com.pivotal.gemfire&
SYS                 |GET_TABLE_VERSION             |com.pivotal.gemfire&
SYS                 |HDFS_LAST_MAJOR_COMPACTION    |com.pivotal.gemfire&
SYSCS_UTIL          |CHECK_TABLE                   |com.pivotal.gemfire&
SYSCS_UTIL          |GET_DATABASE_PROPERTY         |com.pivotal.gemfire&
SYSCS_UTIL          |GET_EXPLAIN_CONNECTION        |com.pivotal.gemfire&
SYSCS_UTIL          |GET_RUNTIMESTATISTICS         |com.pivotal.gemfire&
SYSCS_UTIL          |GET_USER_ACCESS               |com.pivotal.gemfire&
SYSIBM              |BLOBCREATELOCATOR             |com.pivotal.gemfire&
SYSIBM              |BLOBGETBYTES                  |com.pivotal.gemfire&
SYSIBM              |BLOBGETLENGTH                 |com.pivotal.gemfire&
SYSIBM              |BLOBGETPOSITIONFROMBYTES      |com.pivotal.gemfire&
SYSIBM              |BLOBGETPOSITIONFROMLOCATOR    |com.pivotal.gemfire&
SYSIBM              |CLOBCREATELOCATOR             |com.pivotal.gemfire&
SYSIBM              |CLOBGETLENGTH                 |com.pivotal.gemfire&
SYSIBM              |CLOBGETPOSITIONFROMLOCATOR    |com.pivotal.gemfire&
SYSIBM              |CLOBGETPOSITIONFROMSTRING     |com.pivotal.gemfire&
SYSIBM              |CLOBGETSUBSTRING              |com.pivotal.gemfire&
```

<a id= importedkeys> </a>
<h4>**SHOW IMPORTEDKEYS**</h4>

Displays all foreign keys in the specified schema or table. If you omit the schema and table clauses, TIBCO ComputeDB displays all foreign keys for all tables in the current schema.

**Example**

```pre
snappy> show importedkeys in app;
PKTABLE_NAME   |PKCOLUMN_NAME   |PK_NAME              |FKTABLE_SCHEM   |FKTABLE_NAME   |FKCOLUMN_NAME   |FK_NAME        |KEY_SEQ
-------------------------------------------------------------------------------------------------------------------------------- 
CUSTOMERS      |CID             |SQL180328162510710   |TRADE           |BUYORDERS      |CID             |BO_CUST_FK     |1
SECURITIES     |SEC_ID          |SEC_PK               |TRADE           |BUYORDERS      |SID             |BO_SEC_FK      |1
CUSTOMERS      |CID             |SQL180328162510710   |TRADE           |NETWORTH       |CID             |CUST_NEWT_FK   |1
PORTFOLIO      |CID             |PORTF_PK             |TRADE           |SELLORDERS     |CID             |PORTF_FK       |1
PORTFOLIO      |SID             |PORTF_PK             |TRADE           |SELLORDERS     |SID             |PORTF_FK       |2
CUSTOMERS      |CID             |SQL180328162510710   |TRADE           |PORTFOLIO      |CID             |CUST_FK        |1
SECURITIES     |SEC_ID          |SEC_PK               |TRADE           |PORTFOLIO      |SID             |SEC_FK         |1
EMPLOYEES      |EID             |EMPLOYEES_PK         |TRADE           |TRADES         |EID             |EMP_FK         |1
CUSTOMERS      |CID             |SQL180328162510710   |TRADE           |TRADES         |CID             |SQL18032816254&|1

snappy> show importedkeys from app.BUYORDERS;
PKTABLE_NAME   |PKCOLUMN_NAME   |PK_NAME              |FKTABLE_SCHEM   |FKTABLE_NAME   |FKCOLUMN_NAME   |FK_NAME       |KEY_SEQ
---------------------------------------------------------------------------------------------------------------- 
CUSTOMERS      |CID             |SQL180328162510710   |TRADE           |BUYORDERS      |CID             |BO_CUST_FK    |1
SECURITIES     |SEC_ID          |SEC_PK               |TRADE           |BUYORDERS      |SID             |BO_SEC_FK     |1

2 rows selected
```
<a id= indexes> </a>
<h4>**SHOW INDEXES**</h4>

Displays all the indexes in the database.

If `IN schemaName` is specified, only the indexes in the specified schema are displayed. If `FROM table-Name` is specified, only the indexes on the specified table are displayed.

**Example**

```pre
snappy> show indexes in app;
TABLE_NAME          |COLUMN_NAME         |NON_U&|TYPE|ASC&|CARDINA&|PAGES
---------------------------------------------------------------------------- 
AIRLINES            |AIRLINE             |false |3   |A   |NULL    |NULL
CITIES              |CITY_ID             |false |3   |A   |NULL    |NULL
CITIES              |COUNTRY_ISO_CODE    |true  |3   |A   |NULL    |NULL
COUNTRIES           |COUNTRY_ISO_CODE    |false |3   |A   |NULL    |NULL
COUNTRIES           |COUNTRY             |false |3   |A   |NULL    |NULL
FLIGHTAVAILABILITY  |FLIGHT_ID           |false |3   |A   |NULL    |NULL
FLIGHTAVAILABILITY  |SEGMENT_NUMBER      |false |3   |A   |NULL    |NULL
FLIGHTAVAILABILITY  |FLIGHT_DATE         |false |3   |A   |NULL    |NULL
FLIGHTAVAILABILITY  |FLIGHT_ID           |true  |3   |A   |NULL    |NULL
FLIGHTAVAILABILITY  |SEGMENT_NUMBER      |true  |3   |A   |NULL    |NULL
FLIGHTS             |FLIGHT_ID           |false |3   |A   |NULL    |NULL
FLIGHTS             |SEGMENT_NUMBER      |false |3   |A   |NULL    |NULL
FLIGHTS             |DEST_AIRPORT        |true  |3   |A   |NULL    |NULL
FLIGHTS             |ORIG_AIRPORT        |true  |3   |A   |NULL    |NULL
MAPS                |MAP_ID              |false |3   |A   |NULL    |NULL
MAPS                |MAP_NAME            |false |3   |A   |NULL    |NULL

16 rows selected
snappy> show indexes from flights;
TABLE_NAME          |COLUMN_NAME         |NON_U&|TYPE|ASC&|CARDINA&|PAGES
---------------------------------------------------------------------------- 
FLIGHTS             |FLIGHT_ID           |false |3   |A   |NULL    |NULL
FLIGHTS             |SEGMENT_NUMBER      |false |3   |A   |NULL    |NULL
FLIGHTS             |DEST_AIRPORT        |true  |3   |A   |NULL    |NULL
FLIGHTS             |ORIG_AIRPORT        |true  |3   |A   |NULL    |NULL

4 rows selected
```
<a id= procedures> </a>
<h4>**SHOW PROCEDURES**</h4>

SHOW PROCEDURES displays all the procedures in the database that have been created with the CREATE PROCEDURE statement, as well as system procedures.

If `IN schemaName` is specified, only procedures in the specified schema are displayed.

**Example**

``` pre
snappy> show procedures in syscs_util;
PROCEDURE_SCHEM     |PROCEDURE_NAME                |REMARKS             
------------------------------------------------------------------------ 
SYSCS_UTIL          |BACKUP_DATABASE               |com.pivotal.gemfire&
SYSCS_UTIL          |BACKUP_DATABASE_AND_ENABLE_LO&|com.pivotal.gemfire&
SYSCS_UTIL          |BACKUP_DATABASE_AND_ENABLE_LO&|com.pivotal.gemfire&
SYSCS_UTIL          |BACKUP_DATABASE_NOWAIT        |com.pivotal.gemfire&
SYSCS_UTIL          |BULK_INSERT                   |com.pivotal.gemfire&
SYSCS_UTIL          |CHECKPOINT_DATABASE           |com.pivotal.gemfire&
SYSCS_UTIL          |COMPRESS_TABLE                |com.pivotal.gemfire&
SYSCS_UTIL          |DISABLE_LOG_ARCHIVE_MODE      |com.pivotal.gemfire&
SYSCS_UTIL          |EMPTY_STATEMENT_CACHE         |com.pivotal.gemfire&
SYSCS_UTIL          |EXPORT_QUERY                  |com.pivotal.gemfire&
SYSCS_UTIL          |EXPORT_QUERY_LOBS_TO_EXTFILE  |com.pivotal.gemfire&
SYSCS_UTIL          |EXPORT_TABLE                  |com.pivotal.gemfire&
SYSCS_UTIL          |EXPORT_TABLE_LOBS_TO_EXTFILE  |com.pivotal.gemfire&
SYSCS_UTIL          |FREEZE_DATABASE               |com.pivotal.gemfire&
SYSCS_UTIL          |IMPORT_DATA                   |com.pivotal.gemfire&
SYSCS_UTIL          |IMPORT_DATA_EX                |com.pivotal.gemfire&
SYSCS_UTIL          |IMPORT_DATA_LOBS_FROM_EXTFILE |com.pivotal.gemfire&
SYSCS_UTIL          |IMPORT_TABLE                  |com.pivotal.gemfire&
SYSCS_UTIL          |IMPORT_TABLE_EX               |com.pivotal.gemfire&
SYSCS_UTIL          |IMPORT_TABLE_LOBS_FROM_EXTFILE|com.pivotal.gemfire&
SYSCS_UTIL          |INPLACE_COMPRESS_TABLE        |com.pivotal.gemfire&
SYSCS_UTIL          |RELOAD_SECURITY_POLICY        |com.pivotal.gemfire&
SYSCS_UTIL          |SET_DATABASE_PROPERTY         |com.pivotal.gemfire&
SYSCS_UTIL          |SET_EXPLAIN_CONNECTION        |com.pivotal.gemfire&
SYSCS_UTIL          |SET_RUNTIMESTATISTICS         |com.pivotal.gemfire&
SYSCS_UTIL          |SET_STATEMENT_STATISTICS      |com.pivotal.gemfire&
SYSCS_UTIL          |SET_STATISTICS_TIMING         |com.pivotal.gemfire&
SYSCS_UTIL          |SET_USER_ACCESS               |com.pivotal.gemfire&
SYSCS_UTIL          |UNFREEZE_DATABASE             |com.pivotal.gemfire&


29 rows selected

```
<a id= schemas> </a>
<h4>**SHOW SCHEMAS**</h4>

SHOW SCHEMAS displays all of the schemas in the current connection.

**Example**

```pre
snappy> create schema sample;

snappy> show schemas;
TABLE_SCHEM
------------------------------ 
APP
NULLID
SAMPLE
SQLJ
SYS
SYSCAT
SYSCS_DIAG
SYSCS_UTIL
SYSFUN
SYSIBM
SYSPROC
SYSSTAT

12 rows selected
```

<a id= tables> </a>
<h4>**SHOW TABLES**</h4>

SHOW TABLES displays all of the tables in the current schema.

If `IN schemaName` is specified, the tables in the given schema are displayed.

**Example**

```pre
snappy> show tables in app;
TABLE_SCHEM         |TABLE_NAME                    |TABLE_TYPE     |REMARKS
------------------------------------------------------------------------ 
APP                 |AIRLINES                      |COLUMN TABLE    |
APP                 |CITIES                        |COLUMN TABLE    |
APP                 |COUNTRIES                     |COLUMN TABLE    |
APP                 |FLIGHTAVAILABILITY            |EXTERNAL TABLE  |
APP                 |FLIGHTS                       |EXTERNAL TABLE  |
APP                 |FLIGHTS_HISTORY               |ROW TABLE       |
APP                 |MAPS                          |ROW TABLE       |

7 rows selected
snappy>
```
<a id= views> </a>
<h4>**SHOW VIEWS**</h4>

SHOW VIEWS displays all of the views in the current schema.

If `IN schemaName` is specified, the views in the given schema are displayed.

**Example**

```pre
snappy> create view v1 as select * from maps;

snappy> show views;
TABLE_SCHEM         |TABLE_NAME                    |TABLE_TYPE|REMARKS             
----------------------------------------------------------------------------------- 
APP                 |V1                            |VIEW      |                    

1 row selected

snappy> show views in APP;
TABLE_SCHEM         |TABLE_NAME                    |TABLE_TYPE|REMARKS             
----------------------------------------------------------------------------------- 
APP                 |V1                            |VIEW      |                    
APP                 |V2                            |VIEW      |                    

2 rows selected
```
!!!Note
	SHOW VIEWS do not display the temporary and global temporary views.

**Related Topics**</br>

* [SET CONNECTION](set_connection.md)

* [CREATE FUNCTION](../sql_reference/create-function.md)

* [CREATE IMPORTEDKEY]()

* [CREATE INDEXES](../sql_reference/create-index.md)

* [CREATE PROCEDURE]()

* [CREATE SCHEMA](../sql_reference/create-schema/)

* [CREATE TABLE](../sql_reference/create-table.md)

* [CREATE VIEW]()
