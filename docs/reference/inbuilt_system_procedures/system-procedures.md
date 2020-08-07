# Built-in System Procedures and Built-in Functions

TIBCO ComputeDB provides built-in system procedures to help you manage the distributed system. For example, you can use system procedures to install required JAR files.

TIBCO ComputeDB also supports built-in function, which lets you perform various kinds of data transformations directly in SELECT statements.

It also provides the dsid() function built into it that are always available. 
<!--
!!! Note
	If you enable SQL authorization, you must use the [GRANT](/reference/sql_reference/grant.md) command to grant normal users permission to use these procedures. 
	
The following built-in procedures are available:

* [SYS.DUMP_STACKS](dump-stacks.md)

* [SYS.REBALANCE_ALL_BUCKETS](rebalance-all-buckets.md)

* [SYS.SET_CRITICAL_HEAP_PERCENTAGE](set_critical_heap_percentage.md)

* [SYS.SET_TRACE_FLAG](set-trace-flag.md)

The following built-in function is available:

* [DSID](dsid.md)
