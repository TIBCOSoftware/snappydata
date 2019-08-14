# SYS.REMOVE_METASTORE_ENTRY

This procedure drops a table from the external catalog if it exists (without checking that the table exists in the catalog). However, it does not handle related policies and base tables; hence they must be dropped separately.

You must connect to the server from the Snappy shell, check the policies and base tables, and drop them separately.

```
// Viewing and dropping policies
SELECT * FROM SYS.SYSPOLICIES;
DROP POLICY <policy name>;
```

## Syntax

```
call sys.REMOVE_METASTORE_ENTRY('<dbName>.<tableName>', '<forceDrop boolean>');
```

## Example

Considering the case when cluster fails to come up, and the log mentions the following pattern: 
`AnalysisException: Table <dbName>.<tableName> might be inconsistent in hive catalog. Use system procedure SYS.REMOVE_METASTORE_ENTRY to remove inconsistency`, then use the following steps to resolve the issue:

1.	Check if the server is running:

           cd $SNAPPY_HOME
           ./sbin/snappy-status-all.sh

2.	Launch Snappy shell and connect to the server:

			./bin/snappy ;  connect client '<server hostname>:<server port>';

3.	Check and find if there are any policies on the table which caused catalog inconsistency and drop policies:

            SELECT * FROM SYS.SYSPOLICIES;
            DROP POLICY <policy name>;

4.	Call the procedure to drop the table from the catalog:

			call sys.REMOVE_METASTORE_ENTRY('<dbName>.<tableName>', 'false');

5.	Restart SnappyData cluster and check the status. The cluster starts successfully.
