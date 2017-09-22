# Using Trace Flags for Advanced Debugging


SnappyData provides debug trace flags to record additional information about SnappyData features in the log file.

<a id="trace-flag"></a>
SnappyData provides these trace flags that you can use with the `snappydata.debug.true` system property to log additional details about SnappyData behavior:

| Trace flag                 | Enables                                                                                                                                                              |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| QueryDistribution          | Detailed logging for distributed queries and DML statements, including information about message distribution to SnappyData members and scan types that were opened. |
| StatementMatching          | Logging for optimizations that are related to unprepared statements.                                                                                                 |
| TraceAuthentication        | Additional logging for SnappyData authentication.                                                                                                                    |
| TraceDBSynchronizer        | DBSynchronizer and WAN distribution logging.                                                                                                                         |
| TraceClientConn            | Client-side connection open and close stack traces.                                                                                                                  |
| TraceClientStatement       | Client-side, basic timing logging.                                                                                                                                   |
| TraceClientStatementMillis | Client-side wall clock timing.                                                                                                                                       |
| TraceIndex                 | Detailed index logging.                                                                                                                                              |
| TraceJars                  | Logging for JAR installation, replace, and remove events.                                                                                                            |
| TraceTran                  | Detailed logging for transaction events and operations, including commit and rollback.                                                                               |
| TraceLock\_\*              | Locking and unlocking information for all internal locks.                                                                                                            |
| TraceLock\_DD              | Logging for all DataDictionary and table locks that are acquired or released.                                                                                        |

If you are asked to set a trace flag in a SnappyData member for debugging purposes, do so using the `snappydata.debug.true` system property when you start a server or locator. For example, this command sets both the QueryDistribution and TraceIndex flags:

``` pre
snappy-shell snappydata server start -J-Dsnappydata.debug.true=QueryDistribution,TraceIndex
```

For applications that use the JDBC peer driver, set trace-related flags in the snappydata.debug.true system property before the application connects to the SnappyData cluster. For example, this code sets traces for index and query distribution and then connects using the peer driver:

``` pre
Properties props = new Properties();
props.put("snappydata.debug.true", "TraceIndex,QueryDistribution");
System.setProperties(props);
Java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:snappydata:");
```

Client-side trace flags write additional information to the client log file, which is configured using the <a href="../reference/configuration/ConnectionAttributes.html#jdbc_connection_attributes__section_F659D85981384E9AAE128AF6B95A173C" class="xref noPageCitation">snappydata.client.log-file</a> property.

If you need to set a trace flag in a running system, use the <a href="../reference/system_procedures/set-trace-flag.html#reference_A7533A4A873D48FBAB05A67DD5CC7F66" class="xref noPageCitation" title="Enables a snappydata.debug.true trace flag on all members of the distributed system, including locators.">SYS.SET\_TRACE\_FLAG</a> system procedure. The procedure sets the trace flag in all members of the distributed system, including locators. You must execute the procedure as a system user. For example:

``` pre
snappy> call sys.set_trace_flag('traceindex', 'true');
Statement executed.
```


