## Table of Contents
* [Introduction](#introduction)
* [Download binary distribution](#download-binary-distribution)
* [Community Support](#community-support)
* [Link with SnappyData distribution](#link-with-snappydata-distribution)
* [Working with SnappyData Source Code](#working-with-snappydata-source-code)
    * [Building SnappyData from source](#building-snappydata-from-source)
* [Key Features](#key-features)
* [Getting started](#getting-started)
  * [Objectives](#objectives)
  * [Start the SnappyData Cluster](#start-the-snappydata-cluster)
    * [Step 1 - Start the SnappyData cluster](#step-1---start-the-snappydata-cluster)
  * [Interacting with SnappyData](#interacting-with-snappydata)
  * [SnappyData with SQL](#snappydata-with-sql)
    * [Column and Row tables](#column-and-row-tables)
    * [Step 2 - Create column table, row table and load data](#step-2---create-column-table-row-table-and-load-data)
    * [OLAP and OLTP queries](#olap-and-oltp-queries)
    * [Step 3 - Run OLAP and OLTP queries](#step-3---run-olap-and-oltp-queries)
    * [Approximate query processing (AQP)](#approximate-query-processing-aqp)
    * [Step 4 - Create, Load and Query Sample Table](#step-4---create-load-and-query-sample-table)
  * [SnappyData with Spark API](#snappydata-with-spark-api)
    * [Column and Row tables](#column-and-row-tables-1)
    * [Step 2 - Create column table, row table and load data](#step-2---create-column-table-row-table-and-load-data-1)
    * [OLAP and OLTP Store](#olap-and-oltp-store)
    * [Step 3 - Run OLAP and OLTP queries](#step-3---run-olap-and-oltp-queries-1)
    * [Approximate query processing (AQP)](#approximate-query-processing-aqp-1)
    * [Step 4 - Create, Load and Query Sample Table](#step-4---create-load-and-query-sample-table-1)
    * [Stream analytics using SQL and Spark Streaming](#stream-analytics-using-sql-and-spark-streaming)
  * [Note: More TODOs for Hemant](#note-more-todos-for-hemant)

## Introduction
SnappyData is a **distributed in-memory data store for real-time operational analytics, delivering stream analytics, OLTP(online transaction processing) and OLAP(online analytical processing) in a single integrated cluster**. We realize this platform through a seamless integration of Apache Spark (as a big data computational engine) with GemFire XD(as an in- memory transactional store with scale-out SQL semantics). 

![SnappyDataOverview](https://prismic-io.s3.amazonaws.com/snappyblog/c6658eccdaf158546930376296cd7c3d33cff544_jags_resize.png)

## Download binary distribution
You can download the latest version of SnappyData from [here][2]. SnappyData has been tested on Linux (mention kernel version) and Mac (OS X 10.9 and 10.10?). If not already installed, you will need to download scala 2.10 and [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html).  (this info should also be in the download page on our web site) [Skip to Getting Started](#getting-started)

## Community Support

We monitor channels listed below for comments/questions. We prefer using Stackoverflow. 

[Stackoverflow](http://stackoverflow.com/questions/tagged/snappydata) ![Stackoverflow](http://i.imgur.com/LPIdp12.png)    [Slack](http://snappydata-slackin.herokuapp.com/)![Slack](http://i.imgur.com/h3sc6GM.png)        Gitter ![Gitter](http://i.imgur.com/jNAJeOn.jpg)          [IRC](http://webchat.freenode.net/?randomnick=1&channels=%23snappydata&uio=d4) ![IRC](http://i.imgur.com/vbH3Zdx.png)             [Reddit](https://www.reddit.com/r/snappydata) ![Reddit](http://i.imgur.com/AB3cVtj.png)          JIRA ![JIRA](http://i.imgur.com/E92zntA.png)

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

In this document, we discuss various concepts and then ask you take **actions** that showcase these concepts. 

> Note: Discuss local mode:  Like Spark, SnappyData can also be run in [“local” mode](http://spark.apache.org/docs/latest/programming-guide.html#local-vs-cluster-modes).  

###Start the SnappyData Cluster
SnappyData, a database server cluster, has three main components - Locator, Server and Lead. Details of these components can be found [here](docs/architecture.md). 

![ClusterArchitecture](docs/GettingStarted_Architecture.png)

#### Step 1 - Start the SnappyData cluster 

> Note : The quick start scripts use ssh to start up various processes. By default, this requires a password. To be able to log on to the localhost and run the script without being prompted for the password, please enable passwordless ssh 

```
$ sbin/snappy-start-all.sh 
  (Roughly can take upto a minute. Associated logs are in the ‘work’ sub-directory)
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
This script starts up a minimal set of essential components to form the cluster - A locator, one data server and one lead node. All nodes are started locally. To spin up remote nodes simply rename/copy the files without the template suffix and add the hostnames. The [docs_config]() discusses the custom configuration and startup options. 

At this point, the SnappyData cluster is up and running and is ready to accept Snappy jobs and SQL requests via JDBC/ODBC. You can check the state of the cluster using [Pulse](link) - a graphical dashboard for monitoring vital, real-time health and performance of SnappyData members. You can also check the details of the embedded Spark driver using [Spark UI](localhost:4040)

### Interacting with SnappyData

> We assume some familiarity with [core Spark, Spark SQL and Spark Streaming concepts](http://spark.apache.org/docs/latest/). 
> And, you can try out the Spark [Quick Start](http://spark.apache.org/docs/latest/quick-start.html). All the commands and programs
> listed in the Spark guides will work in SnappyData also.

To interact with SnappyData, we provide interfaces for developers familiar with Spark programming as well as SQL. JDBC can be used to connect to SnappyData cluster and interact using SQL. On the other hand, users comfortable with Spark programming paradigm can write Snappy jobs to interact with SnappyData. Snappy jobs can be like a self contained Spark application or can share state with other jobs using SnappyData store. 

Unlike Apache Spark, which is primarily a computational engine, SnappyData cluster holds mutable database state in its JVMs and requires all submitted Spark jobs/queries to share the same state (of course, with schema isolation and security as expected in a database). This required extending Spark in two fundamental ways.

1. __Long running executors__: Executors are running within the Snappy store JVMs and form a p2p cluster.  Unlike Spark, the application Job is decoupled from the executors - submission of a job does not trigger launching of new executors. 
2. __Driver runs in HA configuration__: Assignment of tasks to these executors are managed by the Spark Driver.  When a driver fails, this can result in the executors getting shutdown, taking down all cached state with it. Instead, we leverage the [Spark JobServer](https://github.com/spark-jobserver/spark-jobserver) to manage Jobs and queries within a "lead" node.  Multiple such leads can be started and provide HA (they automatically participate in the SnappyData cluster enabling HA). 
Read [docs](docs) for details of the architecture.

In this document, we showcase mostly the same set of features via Spark API or using SQL. You can skip the SQL part if you are familiar with Scala and Spark. 
> #### Note
> The U.S. Department of Transportation's (DOT) Bureau of Transportation Statistics (BTS) tracks the on-time performance of domestic flights operated by large air carriers. 
Summary information on the number of on-time, delayed, canceled and diverted flights is available for the last 20 years. We use this data set in the examples below. You can learn more on this schema [here](http://www.transtats.bts.gov/Fields.asp?Table_ID=236)

### SnappyData with SQL

For SQL, the SnappyData SQL Shell (_snappy-shell_) provides a simple way to inspect the catalog,  run admin operations,  manage the schema and run interactive queries. You can also use your favorite SQL tool like SquirrelSQL or DBVisualizer( JDBC to connect to the cluster).

```sql
// from the SnappyData base directory
$ ./bin/snappy-shell
Version 2.0-SNAPSHOT.1
snappy> 

-- Connect to the cluster ..
snappy> connect client 'localhost:1527';
snappy> show connections; 

-- Check the cluster status
this will list each cluster member and its status
snappy> show members;
```
#### Column and Row tables 

[Column tables](columnTables) organize and manage data in memory in compressed columnar form such that modern day CPUs can traverse and run computations like a sum or a average really fast (as the values are available in contiguous memory). Column table follows the Spark DataSource access model.
```sql
-- DDL to create a column table
CREATE TABLE AIRLINE (<column definitions>) USING column OPTIONS(buckets '5') ;
```
[Row tables](rowTables), unlike column tables are laid out one row at a time in contiguous memory. Rows are typically accessed using keys and its location determined by a hash function and hence very fast for point lookups or updates.  
_create table_ DDL allows tables to be partitioned on primary keys, custom partitioned, replicated, carry indexes in memory, persist to disk , overflow to disk, be replicated for HA, etc.  Read our preliminary [docs](docs) for the details.
```sql
-- DDL to create a row table
CREATE TABLE AIRLINEREF (<column definitions>) USING row OPTIONS() ;
```
#### Step 2 - Create column table, row table and load data
> 
> Default airline data shipped with product is of 15 MB compressed size. To download the larger data set run this command from the shell:
>> $ ./download_full_airlinedata.sh destFolder

> Then, change the 'create_and_load_column_table.sql' script to point at this data set.


SQL scripts to create and load column and row tables.
```sql
-- Loads parquet formatted data into a temporary spark table 
-- then saves it in  column table called Airline.
snappy> run './quickstart/scripts/create_and_load_column_table.sql';
snappy> select count(*) from airline; 

-- Creates the airline code table. Row tables can be replicated to each node 
-- so join processing with other tables can completely avoid shuffling 
snappy> run './quickstart/scripts/create_and_load_row_table.sql';
snappy> select count(*) from airlineref; //row count 

-- See the status of system
snappy> run './quickstart/scripts/status_queries.sql'
```
#### OLAP and OLTP queries
SQL client connections (via JDBC or ODBC) are routed to the appropriate data server via the locator (Physical connections are automatically created in the driver and are transparently swizzled in case of failures also). When queries are executed they are parsed initially by the SnappyData server to determine if it is a OLAP class or a OLTP class query.  Currently, all column table queries are considered OLAP.  Such queries are routed to the __lead__ node where a __ Spark SQLContext__ is managed for each connection. The Query is planned using Spark's Catalyst engine and scheduled to be executed on the data servers. The number of partitions determine the number of concurrent tasks used across the data servers to parallel run the query. In this case, our column table was created using _5 partitions(buckets)_ and hence will use 5 concurrent tasks. 

```sql
---- Which Airlines Arrive On Schedule? JOIN with reference table ----
snappy> select AVG(ArrDelay) arrivalDelay, description AirlineName, UniqueCarrier carrier 
  from airline_sample, airlineref
  where airline_sample.UniqueCarrier = airlineref.Code 
  group by UniqueCarrier, description 
  order by arrivalDelay;
```
For low latency OLTP queries, the engine won't route it to the lead and instead execute it immediately without any scheduling overhead. Quite often, this may mean simply fetching a row by hashing a key (in nanoseconds).
 
```sql
--- Suppose a particular Airline company say 'Delta Air Lines Inc.' re-brands itself as 'Delta America'
--- the airline code can be updated in the row table
UPDATE AIRLINEREF SET DESCRIPTION='Delta America' WHERE CAST(CODE AS VARCHAR(25))='DL';
```
Spark SQL can cache DataFrames as temporary tables and the data set is immutable. SnappyData SQL is compatible with the SQL standard with support for transactions and DML (insert, update, delete) on tables. [Link to GemXD SQL reference](http://gemxd).  As we show later, any table in Snappy is also visible as Spark DataFrame. 

#### Step 3 - Run OLAP and OLTP queries
 
```sql
-- Simply run the script or copy/paste one query at a time if you want to explore the query execution on the Spark console. 
snappy> run './quickstart/scripts/olap_queries.sql';

---- Which Airlines Arrive On Schedule? JOIN with reference table ----
select AVG(ArrDelay) arrivalDelay, description AirlineName, UniqueCarrier carrier 
  from airline_sample, airlineref
  where airline_sample.UniqueCarrier = airlineref.Code 
  group by UniqueCarrier, description 
  order by arrivalDelay;
```

You can explore the [Spark SQL query plan](http://localhost:4040/jobs/). Each query is executed as a Job and you can explore the different stages of the query execution. (Todo: Add more details here .. image?).
(Todo: If the storage tab will not work, suggest user to peek at the memory used using Jconsole?)

```sql
-- Run a simple update SQL statement on the replicated row table.
snappy> run './quickstart/scripts/oltp_queries.sql';
```
You can now re-run olap_queries.sql to see the updated join result set.

> **Note**
> In the current implementation we only support appending to Column tables. Future releases will support all DML operations. 
> You can execute transactions using commands _autocommit off_ and _commit_.  

#### Approximate query processing (AQP)
OLAP queries are expensive as they require traversing through large data sets and shuffling data across nodes. While the in-memory queries above executed in less than a second the response times typically would be much higher with very large data sets. On top of this, concurrent execution for multiple users would also slow things down. Achieving interactive query speed in most analytic environments requires drastic new approaches like AQP.
Similar to how indexes provide performance benefits in traditional databases, SnappyData provides APIs and DDL to specify one or more curated [stratified samples](http://stratifiedsamples) on large tables. 

> #### Note
> We recommend downloading the _onTime airline_ data for 2009-2015 which is about 50 million records. With the above data set (1 million rows) only about third of the time is spent in query execution engine and  sampling is unlikely to show much of any difference in speed.


The following DDL creates a sample that is 3% of the full data set and stratified on 3 columns. The commonly used dimensions in your _Group by_ and _Where_ make us the _Query Column Set_ (strata columns). Multiple samples can be created and queries executed on the base table are analyzed for appropriate sample selection. 

```sql
CREATE SAMPLE TABLE AIRLINE_SAMPLE
   OPTIONS(
    buckets '5',                          -- Number of partitions 
    qcs 'UniqueCarrier, Year_, Month_',   -- QueryColumnSet(qcs): The strata - 3% of each combination of Carrier, 
                                          -- Year and Month are stored as sample
    fraction '0.03',                      -- How big should the sample be
    strataReservoirSize '50',             -- Reservoir sampling to support streaming inserts
    basetable 'Airline')                  -- The parent base table
```
You can run queries directly on the sample table (stored in columnar format) or on the base table. For base table queries you have to specify the _With Error_ constraint indicating to the SnappyData Query processor that a sample can be substituted for the full data set. 

```sql
-- What is the average arrival delay for all airlines for each month?;
snappy> select avg(ArrDelay), Month_ from Airline where ArrDelay >0 
    group by Month_
    with error .05 ;
-- The above query will consult the sample and return an answer if the estimated answer 
-- is at least 95% accurate (here, by default we use a 95% confidence interval). Read [docs](docs) for more details.

-- You can also access the error using built-in functions. 
snappy> select avg(ArrDelay) avgDelay, absolute_error(avgDelay), Month_ 
    from Airline where ArrDelay >0 
    group by Month_
    with error .05 ;
-- TWO PROBLEMS IN CURRENT IMPL: (1) PRESENTED ERROR IS NOT WITHIN RANGE (2) ERROR CONSTRAINT NOT MET .. BUT QUERY RETURNS VALUE
-- The correct answer is within +/- 'error'
-- Consult the docs for access to other related functions like relative_error(), 
--  lower and upper bounds for the error returned. 
```
#### Step 4 - Create, Load and Query Sample Table

```sql
--- Creates and then samples a table from the Airline table 
snappy> run 'create_and_load_sample_table.sql';
```
You can now re-run the previous OLAP queries with an error constraint and compare the results.  You should notice a 10X or larger difference in query execution latency while the results remain nearly accurate. As a reminder, we recommend downloading the larger data set for this exercise.

```sql
-- re-run olap queries with error constraint to automatically use sampling
snappy> run 'olap_approx_queries.sql';
```

### SnappyData with Spark API 

Snappy jobs are the primary mechanism to interact with SnappyData using Spark API. A job implements either SnappySQLJob or SnappyStreamingJob (for streaming applications) trait. 

```scala
class SnappySampleJob implements SnappySQLJob {
  /** Snappy uses this as an entry point to execute Snappy jobs. **/
  def runJob(snc: SnappyContext, jobConfig: Config): Any

  /** SnappyData calls this function to validate the job input and reject invalid job requests **/
  def validate(sc: SnappyContext, config: Config): SparkJobValidation
}
```
The implementation of _runJob_ function of SnappySQLJob can use SnappyContext to interact with SnappyData store to process and store tables. The implementation of runJob of SnappyStreamingJob can use SnappyStreamingContext to create streams and manage the streaming context. The jobs are submitted to lead node of SnappyData over REST API using a _spark-submit_ like utility.  

#### Column and Row tables 

[Column tables](columnTables) organize and manage data in memory in compressed columnar form such that modern day CPUs can traverse and run computations like a sum or a average really fast (as the values are available in contiguous memory). Column table follows the Spark DataSource access model.

```scala
// creating a column table in Snappy job
snappyContext.createTable("AIRLINE", "column", schema, Map("buckets" -> "5"))
```
[Row tables](rowTables), unlike column tables are laid out one row at a time in contiguous memory. Rows are typically accessed using keys and its location determined by a hash function and hence very fast for point lookups or updates.  
_create table_ DDL allows tables to be partitioned on primary keys, custom partitioned, replicated, carry indexes in memory, persist to disk , overflow to disk, be replicated for HA, etc.  Read our preliminary [docs](docs) for the details.
```scala
// creating a row table in Snappy job
val airlineCodeDF = snappyContext.createTable("AIRLINEREF", "row", schema, Map())
```
#### Step 2 - Create column table, row table and load data
> 
> Default airline data shipped with product is of 15 MB compressed size. To download the larger data set run this command from the shell:
>> $ ./download_full_airlinedata.sh destFolder
> Then, set the following parameter before starting the job. 
>> export APP_PROPS="airline_file=destFolder"

SQL scripts to create and load column and row tables.
To do the same thing via Scala API, submit CreateAndLoadAirlineDataJob to create row and column tables. See more details about jobs and job submission [here.](./docs/jobs.md)

```bash
$ bin/snappy-job.sh submit --lead hostNameOfLead:8090 --app-name airlineApp --class  io.snappydata.examples.CreateAndLoadAirlineDataJob --app-jar $SNAPPY_HOME/lib/quickstart-0.1.0-SNAPSHOT.jar
{"status": "STARTED",
  "result": {
    "jobId": "321e5136-4a18-4c4f-b8ab-f3c8f04f0b48",
    "context": "snappyContext1452598154529305363"
  } }

# A JSON with jobId of the submitted job is returned. Use job ID can be used to query the status of the running job. 
$ bin/snappy-job.sh status --lead hostNameOfLead:8090 --job-id 321e5136-4a18-4c4f-b8ab-f3c8f04f0b48"
{ "duration": "17.53 secs",
  "classPath": "io.snappydata.examples.CreateAndLoadAirlineDataJob",
  "startTime": "2016-01-12T16:59:14.746+05:30",
  "context": "snappyContext1452598154529305363",
  "result": "See /home/hemant/snappyhome/work/localhost-lead-1/CreateAndLoadAirlineDataJob.out",
  "status": "FINISHED",
  "jobId": "321e5136-4a18-4c4f-b8ab-f3c8f04f0b48"
}
# Tables are created
```
#### OLAP and OLTP Store

SnappyContext extends SQLContext and adds functionality to work with row and column tables. When queries inside jobs are executed they are parsed initially by the SnappyData server to determine if it is a OLAP class or a OLTP class query.  Currently, all column table queries are considered OLAP. Such queries are planned using Spark's Catalyst engine and scheduled to be executed on the data servers. 
```scala
val resultDF = airlineDF.join(airlineCodeDF,
        airlineDF.col("UniqueCarrier").equalTo(airlineCodeDF("CODE"))).
        groupBy(airlineDF("UniqueCarrier"), airlineCodeDF("DESCRIPTION")).
        agg("ArrDelay" -> "avg").orderBy("avg(ArrDelay)")
```
For low latency OLTP queries in jobs, SnappyData won't schedule these queries instead execute them immediately on SnappyData servers without any scheduling overhead. Quite often, this may mean simply fetching or updating a row by hashing a key (in nanoseconds). 

#### Step 3 - Run OLAP and OLTP queries

You can query the tables using a scala program as well. See AirlineDataJob. 
```bash
$ bin/snappy-job.sh submit --lead hostNameOfLead:8090 --app-name airlineApp  --class  io.snappydata.examples.AirlineDataJob --app-jar $SNAPPY_HOME/lib/quickstart-0.1.0-SNAPSHOT.jar
{ "status": "STARTED",
  "result": {
    "jobId": "1b0d2e50-42da-4fdd-9ea2-69e29ab92de2",
    "context": "snappyContext1453196176725064822"
 } }
$ bin/snappy-job.sh status --lead localhost:8090  --job-id 1b0d2e50-42da-4fdd-9ea2-69e29ab92de2 
{ "duration": "6.617 secs",
  "classPath": "io.snappydata.examples.AirlineDataJob",
  "startTime": "2016-01-19T15:06:16.771+05:30",
  "context": "snappyContext1453196176725064822",
  "result": "See /snappy/work/localhost-lead-1/AirlineDataJob.out",
  "status": "FINISHED",
  "jobId": "1b0d2e50-42da-4fdd-9ea2-69e29ab92de2"
}
```

#### Approximate query processing (AQP)
OLAP jobs are expensive as they require traversing through large data sets and shuffling data across nodes. While the in-memory jobs above executed in less than a second the response times typically would be much higher with very large data sets. On top of this, concurrent execution for multiple users would also slow things down. Achieving interactive query speed in most analytic environments requires drastic new approaches like AQP.
Similar to how indexes provide performance benefits in traditional databases, SnappyData provides APIs to specify one or more curated [stratified samples](http://stratifiedsamples) on large tables. 

> #### Note
> We recommend downloading the _onTime airline_ data for 2009-2015 which is about 50 million records. With the above data set (1 million rows) only about third of the time is spent in query execution engine and  sampling is unlikely to show much of any difference in speed.

The following scala code creates a sample that is 3% of the full data set and stratified on 3 columns. The commonly used dimensions in your _Group by_ and _Where_ make us the _Query Column Set_ (strata columns). Multiple samples can be created and queries executed on the base table are analyzed for appropriate sample selection. 

```
val sampleDF = snappyContext.createTable(sampleTable, 
        "column_sample", // DataSource provider for sample tables
        updatedSchema, Map("buckets" -> "5",
          "qcs" -> "UniqueCarrier, Year_, Month_",
          "fraction" -> "0.03",
          "strataReservoirSize" -> "50",
          "basetable" -> "Airline"
        ))
```
You can run queries directly on the sample table (stored in columnar format) or on the base table. For base table queries you have to specify the _With Error_ constraint indicating to the SnappyData Query processor that a sample can be substituted for the full data set. 

```scala
// Query Snappy Store's Sample table :Which Airlines arrive On schedule? JOIN with reference table
sampleResult = sampleDF.join(airlineCodeDF,
        sampleDF.col("UniqueCarrier").equalTo(airlineCodeDF("CODE"))).
          groupBy(sampleDF("UniqueCarrier"), airlineCodeDF("DESCRIPTION")).
          agg("ArrDelay" -> "avg").orderBy("avg(ArrDelay)")

 // Query Snappy Store's Airline table with error clause.
airlineDF.groupBy(airlineDF("Month_"))
  .agg("ArrDelay" -> "avg")
  .orderBy("Month_").withError(0.05,0.95)
```

#### Step 4 - Create, Load and Query Sample Table

CreateAndLoadAirlineDataJob and AirlineDataJob executed in the previous sections created the sample tables and executed OLAP queries over them. 

> where/how can we show memory utilization with sampling. 

#### Stream analytics using SQL and Spark Streaming
SnappyData extends Spark streaming so stream definitions can be declaratively done using SQL and you can analyze these streams using SQL.  You can also dynamically run SQL queries on these streams. There is no need to learn Spark streaming APIs or statically define all the rules to be executed on these streams. 

The example below consumes tweets, models the stream as a table (so it can be queried) and we then run ad-hoc SQL from remote clients on the current state of the stream (here the window interval is set to 5 seconds). Later,  in the Spark code section we further enhance to showcase "continuous queries" (CQ). Dynamic registration of CQs (from remote clients) will be available in the next release.

```sql
snappy> create stream table tweetstreamtable
       (id long, text string, fullName string, 
      country string, retweets int, hashtag string)
      using twitter_stream options (
        consumerKey '***REMOVED***',
        consumerSecret '***REMOVED***', 
        accessToken '***REMOVED***', 
        accessTokenSecret '***REMOVED***', 
        streamToRows 'io.snappydata.app.streaming.TweetToRowsConverter'
      );
-- Should we showing option that simulates the stream first? Show consuming actual stream only in packaged example?
-- You can also just run script 'create_stream_table.sql'
``` 
> show sample dynamic SQL queries on this stream ....

> NOTE: SnappyData, out-of-the-box, collocates Spark executors and the data store for efficient data intensive computations. 
> But, it may desirable to isolate the computational cluster for other reasons - for instance, a  computationally intensive Map-reduce machine learning algorithm that needs to iterate for a  cache data set repeatedly. 
> To support such scenarios it is also possible to run native Spark jobs that accesses a SnappyData cluster as a storage layer in a parallel fashion. 

> ### Note: More TODOs for Hemant
> - explain and walk thru code to run olap queries using the DF API ... 
> - maybe, run the same using Spark cache ... highlight performance difference?
> - explain and run OLTP code ... what are the additional APIs on top of spark. 
>  - in All of this link to relevant sections in Spark guide.
>  .- Explain the 'runJob' trait? What is a SnappySQLJob?
>  - Mimic the sampling queries using API .... can we show them error related functions also using API
>  - Describe streaming --- this is quite different than SQL counterpart ....  The topK thing is quite unique ... describe the use case and walk thru code, etc.
>  Finally, we go through Spark standalone cluster working with Snappy .... 


-----


