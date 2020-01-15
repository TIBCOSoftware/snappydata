Experimental Features

TIBCO ComputeDB 1.2.0 provides the following features on an experimental basis. These features are included only for testing purposes and are not yet supported officially:

## Authorization for External Tables
You can enable authorization of external tables by setting the system property **CHECK_EXTERNAL_TABLE_AUTHZ** to true when the cluster's security is enabled.
System admin or the schema owner can grant or revoke the permissions on external tables to other users. 
For example: `GRANT ALL ON <external-table> to <user>;`

## Support ad-hoc, Interactive Execution of Scala code
You can execute Scala code using a new CLI script **snappy-scala** that is built with IJ APIs. You can also run it as an SQL command using prefix **exec scala**. 
The Scala code can use any valid/supported Spark API for example, to carry out custom data loading/transformations or to launch a structured streaming job. Since the code is submitted as an SQL command, you can now also use any SQL tool (based on JDBC/ODBC), including Notebook environments, to execute ad-hoc code blocks directly. Prior to this feature, apps were required to use the smart connector or use the TIBCO ComputeDB specific native Zeppelin interpreter. 
**exec scala** command can be secured using the SQL GRANT/REVOKE permissions. System admin (DB owner) can grant or revoke permissions for Scala interpreter privilege.
