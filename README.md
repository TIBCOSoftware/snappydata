## Introduction
SnappyData is a **distributed in-memory data store for real-time operational analytics, delivering stream analytics, OLTP(online transaction processing) and OLAP(online analytical processing) in a single integrated cluster**. We realize this platform through a seamless integration of Apache Spark (as a big data computational engine) with GemFire XD(as an in- memory transactional store with scale-out SQL semantics). 

![SnappyDataOverview](https://prismic-io.s3.amazonaws.com/snappyblog/c6658eccdaf158546930376296cd7c3d33cff544_jags_resize.png)

## Download binary distribution
You can download the latest version of SnappyData from [here][2]. SnappyData has been tested on Linux (mention kernel version) and Mac (OS X 10.9 and 10.10?). If not already installed, you will need to download scala 2.10 and [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html).  (this info should also be in the download page on our web site)

##Community Support

## Link with SnappyData distribution
SnappyData artifacts are hosted in Maven Central. You can add a Maven dependency with the following coordinates:
```
groupId: io.snappydata
artifactId: snappydata_2.10
version: 0.1_preview
```

## Working with SnappyData Source Code
(Info for our download page?)
If you are interested in working with the latest code or contributing to SnappyData development, you can also check out the master branch from Git:
```
Master development branch
git clone https://github.com/SnappyDataInc/snappydata.git

###### 0.1 preview release branch with stability fixes ######
git clone https://github.com/SnappyDataInc/snappydata.git -b 0.1_preview (??)
```

#### Building SnappyData from source
You will find the instructions for building, layout of the code, integration with IDEs using Gradle, etc, [here](docs/build-instructions.md)
> #### NOTE:
> SnappyData is built using Spark 1.6 (build xx) which is packaged as part of SnappyData. While you can build your application using Apache Spark 1.5, you will need to link to Snappy-spark to make  use of the SnappyData extensions. Gradle build tasks are packaged.  


## Key Features
- **100% compatible with Spark**: Use SnappyData as a database as well as use any of the Spark APIs - ML, Graph, etc. on the same data
- **In-memory row and column store**: Run the store collocated in Spark executors (i.e. a single compute and data cluster) or in its own process space (i.e. separate compute and data cluster)
- **SQL standard compliance**: Spark SQL + several SQL extensions: DML, DDL, indexing, constraints.
- **SQL based extensions for streaming processing**: Use native Spark streaming, Dataframe APIs or declaratively specify your streams and how you want it processed. No need to learn Spark APIs to get going with stream processing or its subtleties when processing in parallel.
- **Interactive analytics using Approximate query processing(AQP)**: We introduce multiple synopses techniques through data structures like count-min-sketch and stratified sampling to dramatically reduce the in-memory space requirements and provide true interactive speeds for analytic queries. These structures can be created and managed by developers with little to no statistical background and can be completely transparent to the SQL developer running queries. Error estimators are also integrated with simple mechanisms to get to the errors through built-in SQL functions. 
- **Mutate, transact on data in Spark**: Use SQL to insert, update, delete data in tables(something that you cannot do in Spark). We also provide extensions to Spark’s context so you can mutate data in your spark programs. Any tables in SnappyData is visible as DataFrames without having to maintain multiples copies of your data: cached RDDs in Spark and then separately in your data store. 
- **Optimizations**: Use indexes to improve query performance in the row store (the GemFire SQL optimizer automatically uses in-memory indexes when available) 
- **High availability not just Fault tolerance**: Data is instantly replicated (one at a time or batch at a time) to other nodes in the cluster and is deeply integrated with a membership based distributed system to detect and handle failures instantaneously providing applications with continuous HA.
- **Durability and recovery:** Data can also be managed on disk and automatically recovered. Utilities for backup and restore are bundled. 

Read SnappyData [docs](complete docs) for a more detailed list of all features and semantics. 

## Getting started

###Objectives

- **In-memory Column and Row tables**: Illustrate both SQL syntax and Spark API to create and manage column tables for large data and how row tables can be used for reference data and can be replicated to each node in the cluster. 
- **OLAP, OLTP operations**: We run analytic class SQL queries (full scan with aggregations) on column tables and fully distributed join queries and observe the space requirements as well as the performance of these queries. For OLTP, we run simple update queries - you can note the Spark API extensions to support mutations in Spark. 
- **AQP**: We run the same analytic queries by creating adjunct stratified samples to note the performance difference - can we get close to interactive query performance speeds?
- **Streaming with SQL**: We ingest twitter streams into both a probabilistic data structure for TopK time series analytics and the entire stream (full data set) into a row table. We run both ad-hoc queries on these streams (modeled as tables) as well as showcase our first preview for continuous querying support. The SnappyData value add demonstrated here is simpler, SQL centric abstractions on top of Spark streaming. And, of course, ingestion into the built-in store.

###Start the SnappyData Cluster
Like Spark, SnappyData can also be run in “local” mode(link?).  Here we use a cluster to show how snappyData running as a database server cluster. 
Start the cluster using sbin/snappy-start-all.sh. This script starts up a minimal set of essential components to form the cluster - A locator, one data server and one lead node. See figure below. Details on the architecture is available [here]. Servers and lead are configured to use 1GB of memory by default. 
![ClusterArchitecture](https://drive.google.com/file/d/0B6s-Dkb7LKolcHhPMDhtYjRuTkU/view?usp=sharing)


```All nodes are started locally. To spin up remote nodes simply rename/copy the files without the template suffix and add the hostnames. The [docs_config]() discusses the custom configuration and startup options```

> ####Note
>The quick start scripts use ssh to start up various processes. By default, this requires a password. To be able to log on to the localhost and run the script without being prompted for the password, please execute the following commands from your shell prompt.
```
$ ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa   
$ cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys 
```
Now start the cluster ...

```
$ sbin/snappy-start-all.sh 
  (roughly can take upto a minute. Associated logs are in the ‘work’ sub-directory)
This would output something like this ...
localhost: Starting SnappyData Locator using peer discovery on: 0.0.0.0[10334]
...
localhost: SnappyData Locator pid: 56703 status: running

localhost: Starting SnappyData Server using locators for peer discovery: jramnara-mbpro[10334]   (port used for members to form a p2p cluster)
localhost: SnappyData Server pid: 56819 status: running
localhost:   Distributed system now has 2 members.

localhost: Starting SnappyData Leader using locators for peer discovery: jramnara-mbpro[10334]
localhost: SnappyData Leader pid: 56932 status: running
localhost:   Distributed system now has 3 members.

localhost:   Other members: jramnara-mbpro(56703:locator)<v0>:54414, jramnara-mbpro(56819:datastore)<v1>:39737

```
You can check the state of the cluster using [Pulse](link) - a graphical dashboard for monitoring vital, real-time health and performance of SnappyData members. 

At this point, the SnappyData cluster is up and running and is ready to accept Spark jobs and to SQL requests via JDBC/ODBC.

> We target both developers familiar with Spark programming as well as SQL developers. We showcase mostly the same set of features via Spark API or using SQL. You can skip the SQL part if you are familiar with Scala and Spark. 
### goto [Getting started using Spark++ APIs](linkToSparkGettingStarted)

### Getting stated using SQL

The SnappyData SQL Shell (_snappy-shell_) provides a simple way to inspect the catalog,  run admin operations,  manage the schema and run interactive queries. You can also use your favorite SQL tool like SquirrelSQL or DBVisualizer( JDBC to connect to the cluster)

```sql
// from the SnappyData base directory
$ cd quickstart/scripts
$ ../../bin/snappy-shell
Version 2.0-SNAPSHOT.1
snappy> 

Connect to the cluster ..
snappy> connect client 'localhost:1527';
snappy> show connections; 

Check the cluster status ..
snappy> select id, kind, status, host, port from sys.members;
this will list each cluster member and its status.
```
> #### Note
> The U.S. Department of Transportation's (DOT) Bureau of Transportation Statistics (BTS) tracks the on-time performance of domestic flights operated by large air carriers. 
Summary information on the number of on-time, delayed, canceled and diverted flights is available for the last 20 years. We use this data set in the examples below. You can learn more on this schema [here](http://www.transtats.bts.gov/Fields.asp?Table_ID=236)


#### Create column, row tables and load data
[Column tables](columnTables) organize and manage data in memory in compressed columnar form such that modern day CPUs can traverse and run computations like a sum or a average really fast (as the values are available in contiguous memory). 
```sql
snappy> run 'create_and_load_column_table.sql';
snappy> select count(*) from airline; //row count 

This script must be in the current working directory (scripts).
It first drops the tables if already available, then loads parquet formatted data into a temporary spark table then saves in column table called Airline.
SQL used to create a column table follows the Spark Data source access model:

CREATE TABLE AIRLINE (<column definitions>)
 USING column OPTIONS(buckets '5') ;

Use of standard SQL (i.e. no USING) will result in creation of a Row table.   
```

[Row tables](rowTables), unlike column tables are laid out one row at a time in contiguous memory. Rows are typically accessed using keys and its location determined by a hash function and hence very fast for point lookups or updates.  
_create table_ DDL allows tables to be partitioned on primary keys, custom partitioned, replicated, carry indexes in memory, persist to disk , overflow to disk, be replicated for HA, etc.  Read our preliminary [docs](docs) for the details.

```sql
snappy> run 'create_and_load_row_table.sql';
snappy> select count(*) from airlineref; //row count 

This creates the airline code table containing airline name reference data. And, as a row table it can be replicated to each node so join processing with other partitioned tables can completely avoid data shuffling. 
```


#### Run OLAP, OLTP queries
SQL client connections (via JDBC or ODBC) are routed to the appropriate data server via the locator (Physical connections are automatically created in the driver and are transparently swizzled in case of failures also). When queries are executed they are parsed initially by the SnappyData server to determine if it is a OLAP class or a OLTP class query.  Currently, all column table queries are considered OLAP.  Such queries are routed to the __lead__ node where a __ Spark SQLContext__ is managed for each connection. The Query is planned using Spark's Catalyst engine and scheduled to be executed on the data servers. The number of partitions determine the number of concurrent tasks used across the data servers to parallel run the query. In this case, our column table was created using _5 partitions(buckets)_ and hence will use 5 concurrent tasks. 
For low latency OLTP queries, the engine won't route it to the lead and instead execute it immediately without any scheduling overhead. Quite often, this may mean simply fetching a row by hashing a key (in nanoseconds). 
Let's try to run some of these queries. 
```sql
Simply run the script or copy/paste one query at a time if you want to explore the query execution on the Spark console. 

snappy> run 'olap_queries.sql';
OR
snappy> elapsedtime on;
----------------------------------------------------------------------
---- Which Airlines Arrive On Schedule? JOIN with reference table ----
----------------------------------------------------------------------
select AVG(ArrDelay) arrivalDelay, description AirlineName, UniqueCarrier carrier 
  from airline_sample, airlineref
  where airline_sample.UniqueCarrier = airlineref.Code 
  group by UniqueCarrier, description 
  order by arrivalDelay;

This will print time to execute the query from the shell. 

```

You can explore the [Spark SQL query plan](http://localhost:4040/jobs/). Each query is executed as a Job and you can explore the different stages of the query execution. (Todo: Add more details here .. image?).
(Todo: If the storage tab will not work, suggest user to peek at the memory used using Jconsole?)

Spark SQL can cache DataFrames as temporary tables and the data set is immutable. SnappyData SQL is compatible with the SQL standard with support for transactions and DML (insert, update, delete) on tables. [Link to GemXD SQL reference](http://gemxd).  As we show later, any table in Snappy is also visible as Spark DataFrame. 

```sql
Run a simple update SQL statement on the replicated row table.

snappy> run 'oltp_queries.sql';
```
You can execute transactions using commands _autocommit off_ and _commit_.  
> ####Note
> In the current implementation we only support appending to Column tables. Future releases will support all DML operations. 

#### Approximate query processing (AQP)
OLAP queries tend to be very expensive as this require traversing through large data sets and shuffling data across nodes. While the in-memory queries above executed in less than a second the response times typically would be much higher with very large data sets. On top of this, concurrent execution for multiple users would also slow things down. Achieving interactive query speed in most analytic environments requires drastic new approaches like AQP.
Similar to how indexes provide performance benefits in traditional databases, SnappyData provides APIs (and DDL) to specify one or more curated [stratified samples](http://stratifiedsamples) on large tables. 

> #### Note
> We recommend downloading the _onTime airline_ data for 2009-2015 which is about 50 million records. With the above data set (1 million rows) only about third of the time is spent in query execution engine and  sampling is unlikely to show much of any difference in speed.
> ```
> To download the larger data set run this command from the shell:
> $ ./download_full_airlinedata.sh ../data   (Is this correct?)
> Then, go back to the SQL shell and re-run the 'create_and_load_column_table.sql' script. You could re-run the OLAP queries to note the performance. 

```sql
Execute the following script to create a sample that is 3% of the full data set and stratified on 3 columns. The commonly used dimensions in your _Group by_ and _Where_ make us the _Query Column Set_ (strata columns). Multiple samples can be created and queries executed on the base table are analyzed for appropriate sample selection. 

snappy> run 'create_and_load_sample_table.sql';

Here is the _Create DDL_ for the sample table
CREATE TABLE AIRLINE_SAMPLE
  USING column_sample  //All Sample tables are columnar
  OPTIONS(
    buckets '5',  // Number of partitions 
    qcs 'UniqueCarrier, Year_, Month_', //QueryColumnSet: The strata - 3% of each combination of Carrier, Year and Month is stored as sample
    fraction '0.03', //How big should the sample be
    strataReservoirSize '50', //Reservoir sampling to support streaming inserts
    basetable 'Airline') // The parent base table
  ...
```

> Todo: Provide script file with SQL based on error and confidence .... Hemant?



-----


