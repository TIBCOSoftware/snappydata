#show

Displays information about active connections and database objects.

##Syntax

``` pre
SHOW
{
   CONNECTIONS |
   IMPORTEDKEYS [ IN schemaName | FROM table-Name ] |
   INDEXES [ IN schemaName | FROM table-Name ] |
   PROCEDURES [ IN schemaName ] |
   SCHEMAS |
   SYNONYMS [ IN schemaName ] |
   TABLES [ IN schemaName ] |
   VIEWS [ IN schemaName ] |

}
```

<a id="rtoolsijcomrefshow__section_65A21E80BBA34969BB33823F900A48D3"></a>
##Description


Displays information about active connections and database objects.

**SHOW CONNECTIONS**

If there are no connections, the SHOW CONNECTIONS command returns "No connections available".

Otherwise, the command displays a list of connection names and the URLs used to connect to them. The currently active connection is marked with an \* after its name.

**Example**

``` pre
snappy(CLIENTCONNECTION)> show connections;
CLIENTCONNECTION* -     jdbc:snappydata://localhost:1527/
PEERCLIENT -    jdbc:snappydata:
* = current connection
snappy(CLIENTCONNECTION)>
```

**SHOW IMPORTEDKEYS**

SHOW IMPORTEDKEYS displays all foreign keys in the specified schema or table. If you omit the schema and table clauses, gfxd displays all foreign keys for all tables in the current schema.

**Example**

``` pre
snappy> show importedkeys;
KTABLE_SCHEM |PKTABLE_NAME|PKCOLUMN_NAME   |PK_NAME     |FKTABLE_SCHEM|FKTABLE_NAME      |FKCOLUMN_NAME   |FK_NAME     |KEY_SEQ
-------------------------------------------------------------------------------------------------------------------------------
APP          |COUNTRIES   |COUNTRY_ISO_CODE|COUNTRIES_PK|APP          |CITIES            |COUNTRY_ISO_CODE|COUNTRIES_FK|1      
APP          |FLIGHTS     |FLIGHT_ID       |FLIGHTS_PK  |APP          |FLIGHTAVAILABILITY|FLIGHT_ID       |FLIGHTS_FK2 |1      
APP          |FLIGHTS     |SEGMENT_NUMBER  |FLIGHTS_PK  |APP          |FLIGHTAVAILABILITY|SEGMENT_NUMBER  |FLIGHTS_FK2 |2      

3 rows selected
snappy> show importedkeys from flightavailability;
PKTABLE_NAME|PKCOLUMN_NAME |PK_NAME   |FKTABLE_SCHEM|FKTABLE_NAME      |FKCOLUMN_NAME |FK_NAME    |KEY_SEQ
----------------------------------------------------------------------------------------------------------
FLIGHTS     |FLIGHT_ID     |FLIGHTS_PK|APP          |FLIGHTAVAILABILITY|FLIGHT_ID     |FLIGHTS_FK2|1      
FLIGHTS     |SEGMENT_NUMBER|FLIGHTS_PK|APP          |FLIGHTAVAILABILITY|SEGMENT_NUMBER|FLIGHTS_FK2|2      

2 rows selected
```

**SHOW INDEXES**

SHOW INDEXES displays all the indexes in the database.

If `IN                  schemaName` is specified, only the indexes in the specified schema are displayed.

If `FROM                  table-Name` is specified, only the indexes on the specified table are displayed.

**Example**

``` pre
snappy(localhost:<port number>)> show indexes in app;
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
snappy(localhost:<port number>)> show indexes from flights;
TABLE_NAME          |COLUMN_NAME         |NON_U&|TYPE|ASC&|CARDINA&|PAGES
----------------------------------------------------------------------------
FLIGHTS             |FLIGHT_ID           |false |3   |A   |NULL    |NULL
FLIGHTS             |SEGMENT_NUMBER      |false |3   |A   |NULL    |NULL
FLIGHTS             |DEST_AIRPORT        |true  |3   |A   |NULL    |NULL
FLIGHTS             |ORIG_AIRPORT        |true  |3   |A   |NULL    |NULL

4 rows selected
snappy(localhost:<port number>)>
```

**SHOW PROCEDURES**

SHOW PROCEDURES displays all the procedures in the database that have been created with the CREATE PROCEDURE statement, as well as system procedures.

If `IN                  schemaName` is specified, only procedures in the specified schema are displayed.

**Example**

``` pre
snappy(localhost:<port number>)> show procedures in syscs_util;
PROCEDURE_SCHEM     |PROCEDURE_NAME                |REMARKS
------------------------------------------------------------------------
SYSCS_UTIL          |BACKUP_DATABASE               |com.pivotal.snappydata.&
SYSCS_UTIL          |BACKUP_DATABASE_AND_ENABLE_LO&|com.pivotal.snappydata.&
SYSCS_UTIL          |BACKUP_DATABASE_AND_ENABLE_LO&|com.pivotal.snappydata.&
SYSCS_UTIL          |BACKUP_DATABASE_NOWAIT        |com.pivotal.snappydata.&
SYSCS_UTIL          |BULK_INSERT                   |com.pivotal.snappydata.&
SYSCS_UTIL          |CHECKPOINT_DATABASE           |com.pivotal.snappydata.&
SYSCS_UTIL          |COMPRESS_TABLE                |com.pivotal.snappydata.&
SYSCS_UTIL          |DISABLE_LOG_ARCHIVE_MODE      |com.pivotal.snappydata.&
SYSCS_UTIL          |EMPTY_STATEMENT_CACHE         |com.pivotal.snappydata.&
SYSCS_UTIL          |EXPORT_QUERY                  |com.pivotal.snappydata.&
SYSCS_UTIL          |EXPORT_QUERY_LOBS_TO_EXTFILE  |com.pivotal.snappydata.&
SYSCS_UTIL          |EXPORT_TABLE                  |com.pivotal.snappydata.&
SYSCS_UTIL          |EXPORT_TABLE_LOBS_TO_EXTFILE  |com.pivotal.snappydata.&
SYSCS_UTIL          |FREEZE_DATABASE               |com.pivotal.snappydata.&
SYSCS_UTIL          |IMPORT_DATA                   |com.pivotal.snappydata.&
SYSCS_UTIL          |IMPORT_DATA_LOBS_FROM_EXTFILE |com.pivotal.snappydata.&
SYSCS_UTIL          |IMPORT_TABLE                  |com.pivotal.snappydata.&
SYSCS_UTIL          |IMPORT_TABLE_LOBS_FROM_EXTFILE|com.pivotal.snappydata.&
SYSCS_UTIL          |INPLACE_COMPRESS_TABLE        |com.pivotal.snappydata.&
SYSCS_UTIL          |RELOAD_SECURITY_POLICY        |com.pivotal.snappydata.&
SYSCS_UTIL          |SET_DATABASE_PROPERTY         |com.pivotal.snappydata.&
SYSCS_UTIL          |SET_EXPLAIN_CONNECTION        |com.pivotal.snappydata.&
SYSCS_UTIL          |SET_STATISTICS_TIMING         |com.pivotal.snappydata.&
SYSCS_UTIL          |SET_USER_ACCESS               |com.pivotal.snappydata.&
SYSCS_UTIL          |UNFREEZE_DATABASE             |com.pivotal.snappydata.&

27 rows selected
snappy(localhost:<port number>)>         
```

**SHOW SCHEMAS**

SHOW SCHEMAS displays all of the schemas in the current connection.

**Example**

``` pre
snappy(localhost:<port number>)> create schema sample;
0 rows inserted/updated/deleted
snappy(localhost:<port number>)> show schemas;
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

**SHOW SYNONYMS**

SHOW SYNONYMS displays all synonyms in the database that have been created with the CREATE SYNONYMS statement.

If `IN                  schemaName` is specified, only synonyms in the specified schema are displayed.

**Example**

``` pre
snappy(localhost:<port number>)> SHOW SYNONYMS;
TABLE_SCHEM         |TABLE_NAME                    |REMARKS
------------------------------------------------------------------------

0 rows selected
snappy(localhost:<port number>)> CREATE SYNONYM myairline FOR airlines;
0 rows inserted/updated/deleted
snappy(localhost:<port number>)> SHOW SYNONYMS;
TABLE_SCHEM         |TABLE_NAME                    |REMARKS
------------------------------------------------------------------------
APP                 |MYAIRLINE                     |

1 row selected
snappy(localhost:<port number>)>
```

**SHOW TABLES**

SHOW TABLES displays all of the tables in the current schema.

If `IN                  schemaName` is specified, the tables in the given schema are displayed.

**Example**

``` pre
snappy(localhost:<port number>)> show tables in app;
TABLE_SCHEM         |TABLE_NAME                    |REMARKS
------------------------------------------------------------------------
APP                 |AIRLINES                      |
APP                 |CITIES                        |
APP                 |COUNTRIES                     |
APP                 |FLIGHTAVAILABILITY            |
APP                 |FLIGHTS                       |
APP                 |FLIGHTS_HISTORY               |
APP                 |MAPS                          |

7 rows selected
snappy(localhost:<port number>)>
```

**SHOW VIEWS**

SHOW VIEWS displays all of the views in the current schema.

If `IN                  schemaName` is specified, the views in the given schema are displayed.

**Example**

``` pre
snappy(localhost:<port number>)> create view v1 as select * from maps;
0 rows inserted/updated/deleted
snappy(localhost:<port number>)> show views;
TABLE_SCHEM         |TABLE_NAME                    |REMARKS
------------------------------------------------------------------------
APP                 |V1                            |

1 row selected
```


