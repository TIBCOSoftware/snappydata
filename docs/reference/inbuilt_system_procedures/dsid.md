# DSID

The DSID function returns the string form of the distributed member process identity uniquely represented in the distributed system.

!!! Note:

	* This function is not supported in the Smart Connector mode. It is only supported for embedded mode, JDBC and ODBC.

	*  In some rare cases, if the bucket has just moved while the query was being scheduled, then remote bucket fetch cannot be performed by the query partition but it still displays the DSID() of the node where the partition was executed.

## Example

```	
select dsid(),ECONOMY_SEATS from AIRLINES;

|DSID()                              |ECONOMY_SEATS  |
+----------------------------------+-+
|192.168.1.186(9832:loner):0:acecdb5a|1  |
|192.168.1.189(9832:loner):0:acecdb5a|31 |
|192.168.1.191(9832:loner):0:acecdb5a|87 |
|192.168.1.199(9832:loner):0:acecdb5a|12 |
```





