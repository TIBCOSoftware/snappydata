# DSID

The DSID function returns the string form of the distributed member process identity uniquely represented in the distributed system.

!!! Note

	* This function is not supported in the Smart Connector mode. It is only supported for embedded mode, JDBC and ODBC.

	*  In some rare cases, if the bucket has just moved while the query was being scheduled, then remote bucket fetch cannot be performed by the query partition but it still displays the DSID() of the node where the partition was executed.

## Example

``` pre
snappy>select count(*), dsid() from AIRLINE group by dsid();

count(1)            |DSID()                      
-------------------------------------------------
347749              |192.168.1.98(3625)<v3>:8739 
348440              |192.168.1.98(3849)<v4>:50958
303811              |192.168.1.98(3255)<v1>:2428 

3 rows selected
```

**Also see:**

*	[Built-in System Procedures and Built-in Functions](system-procedures.md)




