# CREATE VIEW

## SYNTAX

```pre
snappy> CREATE VIEW [view-name]  AS SELECT [column_name, column_name] FROM [table_name];

snappy> CREATE VIEW [view-name] (column_name, column_name) AS SELECT column_name, column_name FROM [table_name];
```

## Description

The View can be described as a virtual table that contains a set of definitions, built on top of the table(s) or the other view(s), but it does not physically store the data like a table.
View is persistent and visible in system catalog and therefore shared between all connections.

## Examples </br>

```pre
snappy> CREATE VIEW TRADE.ORDERS1 AS SELECT ORDERID, ITEMID FROM TRADE.ORDERS;

snappy> CREATE VIEW TRADE.ORDERS2 AS SELECT ORDERID, ITEMID FROM TRADE.ORDERS;
```

## CREATE TEMPORARY VIEW

```pre
snappy> CREATE TEMPORARY VIEW [temporary-view-name] AS SELECT [column-name], [column-name] FROM [schema].[table-name];

snappy> CREATE TEMPORARY VIEW [temporary-view-name] USING PARQUET OPTIONS(PATH 'path-to-parquet');
```
### Description
Creates a session-specific temporary view, which is dropped when the session ends.
Temporary views have the same restrictions as permanent views, so you cannot perform insert, update, delete, or copy operations on these views.

Local temporary views are session-scoped; the view drops automatically when the session ends. 

### Examples

```pre
snappy> CREATE TEMPORARY VIEW AIRLINEVIEW1 AS SELECT ORDERID, ITEMID FROM TRADE.ORDERS;

snappy> CREATE TEMPORARY VIEW AIRLINEVIEW2 USING PARQUET OPTIONS(PATH '../../QUICKSTART/DATA/AIRLINEPARQUETDATA');
```

## CREATE GLOBAL TEMPORARY VIEW

```pre
snappy> CREATE GLOBAL TEMPORARY VIEW [global-temporary-view-name] AS SELECT [column-name], [column-name] FROM [schema].[table-name];

snappy> CREATE GLOBAL TEMPORARY VIEW [global-temporary-view-name] USING PARQUET OPTIONS(path 'path-to-parquet');
```

### Description
Creates a global temporary view this is visible to all sessions. Temporary table data is visible only to the session that inserts the data into the table.

The optional GLOBAL keyword allows the view to be shared among all connections but it is not persisted to system catalog so will disappear when lead/driver restarts or fails. Use CREATE VIEW for persistent views.

### Examples

```pre
snappy> CREATE GLOBAL TEMPORARY VIEW ORDER AS SELECT ORDERID, ITEMID FROM TRADE.ORDERS;

snappy> CREATE GLOBAL TEMPORARY VIEW AIRLINEVIEW USING PARQUET OPTIONS(PATH '../../QUICKSTART/DATA/AIRLINEPARQUETDATA');
```
!!! Note
	Temporary views/tables are scoped to SQL connection or the Snappy Spark session that creates it. VIEW or TABLE are synonyms in this context with the former being the preferred usage. This table does not appear in the system catalog nor visible to other connections or sessions.
