# Resolving Catalog Inconsistency Issues

A SnappyData catalog internally maintains table metadata in two catalogs:

*	**Data dictionary** required by SnappyData store
*	**Hive metastore** required by Spark
	
In rare conditions, SnappyData catalog may become inconsistent, if an entry for the table exists only in one of the catalogs instead of exisiting in both.  One of the symptoms for such an inconsistency is that you get an error that indicates that the table you are creating already exists. However, when you drop the same table, the table is not found.

For example:

```
snappy> create table t1(col1 int);
ERROR 42000: (SQLState=42000 Severity=20000) (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-3) Syntax error or analysis exception: createTable: Table APP.T1 already exists.;
snappy> drop table t1;
ERROR 42X05: (SQLState=42X05 Severity=20000) (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-3) Table/View 'APP.T1' does not exist.
```

In such cases, you can use a system procedure, **SYS.REPAIR_CATALOG** (Boolean **removeInconsistentEntries**, Boolean **removeTablesWithData**) to remove catalog inconsistencies.

The following parameters are accepted by the **SYS.REPAIR_CATALOG** procedure:

*	**removeInconsistentEntries** - If **true**, removes inconsistent entries from the catalog.

*	**removeTablesWithData** - If **true**, removes entries for tables even if those tables contain data. By default the entries are not removed.

## Example: Resolving Catalog Inconsistency Issues

When both parameters are set to **false**, the **SYS.REPAIR_CATALOG** procedure checks for any inconsistencies in the catalog and prints warning messages in the SnappyData system server log.

```
snappy> call sys.repair_catalog('false', 'false');
```

In case of inconsistencies, the log will contain messages as shown in the following example:

```
18/08/06 17:28:36.456 IST ThriftProcessor-3<tid=0x82> WARN snappystore: CATALOG: Catalog inconsistency detected: following tables in Hive metastore are not in datadictionary: schema = APP tables = [T1]
18/08/06 17:28:36.457 IST ThriftProcessor-3<tid=0x82> WARN snappystore: CATALOG: Use system procedure SYS.REPAIR_CATALOG() to remove inconsistency

```

You can then remove the catalog inconsistency by passing **removeInconsistentEntries** parameter as **true**.

```
snappy> call sys.repair_catalog('true', 'false');
```

Later, you can examine the log to check which entries were removed. Following is a sample log for reference:

```
18/08/06 17:34:26.548 IST ThriftProcessor-3<tid=0x82> WARN snappystore: CATALOG: Catalog inconsistency detected: following tables in Hive metastore are not in datadictionary: schema = APP tables = [T1]
18/08/06 17:34:26.548 IST ThriftProcessor-3<tid=0x82> WARN snappystore: CATALOG: Removing table APP.T1 from Hive metastore
```
