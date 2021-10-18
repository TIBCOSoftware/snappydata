# Built-in System Procedures and Built-in Functions

SnappyData provides built-in system procedures to help you manage the distributed system. For example, you can use system procedures to install required JAR files.

SnappyData also supports built-in function, which lets you perform various kinds of data transformations directly in SELECT statements.

!!! Note
	If you enable SQL authorization, you must use the [GRANT](../sql_reference/grant.md) command to grant normal users permission to use these procedures.

All system procedures are part of the SYS schema. The following built-in procedures are available:

* [DUMP\_STACKS](dump-stacks.md)

* [REBALANCE\_ALL\_BUCKETS](rebalance-all-buckets.md)

* [SET\_CRITICAL\_HEAP\_PERCENTAGE](set_critical_heap_percentage.md)

* [SET\_TRACE\_FLAG](set-trace-flag.md)

* [REMOVE\_METASTORE\_ENTRY](sys_remove_metastore_entry.md)

* [EXPORT\_DDLS](export_ddl.md)

* [EXPORT\_DATA](export_data.md)

The following built-in functions are available:

* [DSID](dsid.md)

* [CURRENT_USER](current_user.md)

* [CURRENT_USER_LDAP_GROUPS](current_user_ldap_groups.md)
