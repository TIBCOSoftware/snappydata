# ODBC and JDBC Clients 

When using JDBC or ODBC clients, applications must close the ResultSet that is consumed or consume a FORWARD_ONLY ResultSet completely. These ResultSets can keep tables open and thereby block any DDL executions. If the cursor used by ResultSet remains open, then the DDL executions may time out.

Such intermittent ResultSets are eventually cleaned up by the product, but that happens only in a garbage collection (GC) cycle where JVM cleans the weak references corresponding to those ResultSets.
However, this process can take an indeterminate amount of time so it's recommended for users to clean up the ResultSets,
Statements and other JDBC/ODBC constructs immediately after use (using try-resources in Java and equivalent in other languages).
