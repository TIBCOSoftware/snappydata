# Open Source/ Enterprise Components

SnappyData offers a fully functional core OSS distribution that is Apache V2 licensed and includes the following capabilities:

* Mutable row and column store with support for SQL-92 and Spark SQL queries

* 100% compatibility with Apache Spark which allows users to run compute and store data in a single cluster

* Support for shared nothing persistence and high availability of data through replication in the data store

* Support for the REST API to submit Spark jobs and a managed driver which provides fault tolerance for driver failure

* Access to the system using JDBC (the JDBC driver is included in the OSS distribution) or the Spark API

* Command line tools  to backup, restore and export data 

* Extensions to the Spark console which provide useful information on the cluster

* A complete library of statistics and a visual statistics viewer which can be used for both online and offline analysis of system performance and behavior

* Support for transactions and indexing in Row Tables

* SQL Extensions for stream processing

The Enterprise version of the product includes everything that is offered in the OSS version and includes the following additional capabilities that are closed source and only available as part of a licensed subscription. Users can download the Enterprise version for evaluation after registering on the [SnappyData website](https://www.snappydata.io/).

* Synopsis Data Engine capabilities which provides users with the ability to run queries on samples and other summary structures built into the system and get statistically verified accurate results

* ODBC driver that provides high concurrency

* Support for off-heap data storage for column tables which provides higher performance, reduce garbage generation and reduces provisioning and capacity tuning complexity

* CDC Stream receiver that allows data ingestion from Microsoft SQL Server into SnappyData

* SnappyData connector for GemFire/ Apache Geode which allows SnappyData to work across multiple GemFire data grids

* Implementation of the security interface that uses LDAP for authenticating and authorizing users in the system
