# Managing Data Consistency 

Data is frequently required to help users and organizations make business decisions, and therefore it may be important that this data accurately represents the most current information available and that it is consistent. Data consistency implies that all instances of an application are presented with the same set of data values all of the time.

This is achieved using the following two models:
<!--
In the world of relational databases, consistency is often enforced by transactional models that use locks to prevent concurrent application instances from modifying the same data at the same time.

The locks also block concurrent requests to query data, but many relational databases enable an application to relax this rule and provide access to a copy of the data that reflects the state it was in before the update started. 
 
 Many applications that store data in non-relational databases, flat files, or other structures follow a similar strategy, known as pessimistic locking. An application instance locks data while it is being modified, and then releases the lock when the update is complete. -->

**Transactions for Row Tables**

SnappyData supports transaction isolation levels when using JDBC or ODBC connections. Transactions specify an isolation level that defines the degree to which one transaction must be isolated from resource or data modifications made by other transactions. For more information, see [Using Transactions for Row Tables](consistency/using_transactions_row.md)


**Snapshot Isolation for Column Tables**

Snapshot isolation is used for column tables, which ensures that all queries see the same version (snapshot), of the database, based on the state of the database at the moment in time when the query is executed.
For more information, see [Using Snapshot Isolation for Column Tables](consistency/using_snapshot_isolation_column.md)